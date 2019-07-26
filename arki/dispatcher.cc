#include "dispatcher.h"
#include "metadata/consumer.h"
#include "matcher.h"
#include "dataset.h"
#include "dataset/local.h"
#include "validator.h"
#include "types/reftime.h"
#include "types/source.h"
#include "utils/string.h"
#include "utils/sys.h"
#include "nag.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {

static inline Matcher getFilter(const core::cfg::Section& cfg)
{
    return Matcher::parse(cfg.value("filter"));
}

Dispatcher::Dispatcher(const core::cfg::Sections& cfg)
    : m_can_continue(true), m_outbound_failures(0)
{
    // Validate the configuration, and split normal datasets from outbound
    // datasets
    for (const auto& si: cfg)
    {
        if (si.first == "error" or si.first == "duplicates")
            continue;
        else if (si.second.value("type") == "outbound")
        {
            if (si.second.value("filter").empty())
                throw std::runtime_error("configuration of dataset '" + si.first + "' does not have a 'filter' directive");
            outbounds.push_back(make_pair(si.first, getFilter(si.second)));
        }
        else {
            if (si.second.value("filter").empty())
                throw std::runtime_error("configuration of dataset '" + si.first + "' does not have a 'filter' directive");
            datasets.push_back(make_pair(si.first, getFilter(si.second)));
        }
    }
}

Dispatcher::~Dispatcher()
{
}

void Dispatcher::add_validator(const Validator& v)
{
    validators.push_back(&v);
}

void Dispatcher::dispatch(dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    dataset::WriterBatch error_batch;
    dataset::WriterBatch duplicate_batch;
    std::map<std::string, dataset::WriterBatch> by_dataset;
    std::map<std::string, dataset::WriterBatch> outbound_by_dataset;

    // Choose the target dataset(s) for each element in the batch
    for (auto& e: batch)
    {
        // Ensure that we have a reference time
        const reftime::Position* rt = e->md.get<types::reftime::Position>();
        if (!rt)
        {
            using namespace arki::types;
            e->md.add_note("Validation error: reference time is missing, using today");
            // Set today as a dummy reference time, and import into the error dataset
            e->md.set(Reftime::createPosition(Time::create_now()));
            error_batch.push_back(e);
            continue;
        }

        // Run validation
        if (!validators.empty())
        {
            bool validates_ok = true;
            vector<string> errors;

            // Run validators
            for (const auto& v: validators)
                validates_ok = validates_ok && (*v)(e->md, errors);

            // Annotate with the validation errors
            for (const auto& error: errors)
                e->md.add_note("Validation error: " + error);

            if (!validates_ok)
            {
                error_batch.push_back(e);
                continue;
            }
        }

        // See what outbound datasets match this metadata
        for (auto& d: outbounds)
            if (d.second(e->md))
                outbound_by_dataset[d.first].push_back(e);

        // See what regular datasets match this metadata
        vector<string> found;
        for (auto& d: datasets)
            if (d.second(e->md))
                found.push_back(d.first);

        // If we found only one dataset, acquire it in that dataset; else,
        // acquire it in the error dataset
        if (found.empty())
        {
            e->md.add_note("Message could not be assigned to any dataset");
            error_batch.push_back(e);
            continue;
        }

        if (found.size() > 1)
        {
            e->md.add_note("Message matched multiple datasets: " + str::join(", ", found));
            error_batch.push_back(e);
            continue;
        }

        by_dataset[found[0]].push_back(e);
    }

    // Acquire into outbound datasets
    for (auto& b: outbound_by_dataset)
    {
        raw_dispatch_dataset(b.first, b.second, false);
        for (const auto& e: b.second)
            if (e->result != dataset::ACQ_OK)
                // What do we do in case of error?
                // The dataset will already have added a note to the metadata
                // explaining what was wrong.  The best we can do is keep a
                // count of failures.
                ++m_outbound_failures;
    }

    // Acquire into regular datasets
    for (auto& b: by_dataset)
    {
        raw_dispatch_dataset(b.first, b.second, drop_cached_data_on_commit);
        for (auto& e: b.second)
            switch (e->result)
            {
                case dataset::ACQ_OK: break;
                case dataset::ACQ_ERROR_DUPLICATE:
                    // If insertion in the designed dataset failed, insert in the
                    // error dataset
                    duplicate_batch.push_back(e);
                    break;
                case dataset::ACQ_ERROR:
                default:
                    // If insertion in the designed dataset failed, insert in the
                    // error dataset
                    error_batch.push_back(e);
                    break;

            }
    }

    raw_dispatch_dataset("duplicates", duplicate_batch, drop_cached_data_on_commit);
    for (auto& e: duplicate_batch)
        if (e->result != dataset::ACQ_OK)
            // If insertion in the duplicates dataset failed, insert in the
            // error dataset
            error_batch.push_back(e);

    raw_dispatch_dataset("error", error_batch, drop_cached_data_on_commit);
}


RealDispatcher::RealDispatcher(const core::cfg::Sections& cfg)
    : Dispatcher(cfg), datasets(cfg), pool(datasets)
{
}

RealDispatcher::~RealDispatcher()
{
}

void RealDispatcher::raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    if (batch.empty()) return;
    dataset::AcquireConfig cfg;
    cfg.drop_cached_data_on_commit = drop_cached_data_on_commit;
    pool.get(name)->acquire_batch(batch, cfg);
}

void RealDispatcher::flush() { pool.flush(); }



TestDispatcher::TestDispatcher(const core::cfg::Sections& cfg)
    : Dispatcher(cfg), cfg(cfg)
{
    if (!cfg.section("error"))
        throw std::runtime_error("no [error] dataset found");
}
TestDispatcher::~TestDispatcher() {}

void TestDispatcher::raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    if (batch.empty()) return;
    // TODO: forward drop_cached_data_on_commit
    dataset::Writer::test_acquire(*cfg.section(name), batch);
}

void TestDispatcher::dispatch(dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    Dispatcher::dispatch(batch, drop_cached_data_on_commit);
    for (const auto& e: batch)
    {
        if (e->dataset_name.empty())
            nag::verbose("Message %s: not imported", e->md.source().to_string().c_str());
        else
            nag::verbose("Message %s: imported into %s", e->md.source().to_string().c_str(), e->dataset_name.c_str());
        nag::verbose("  Notes:");
        for (const auto& note: e->md.notes())
            nag::verbose("    %s", note.content.c_str());
    }
}

void TestDispatcher::flush()
{
}

}
