#include "dispatcher.h"
#include "configfile.h"
#include "metadata/consumer.h"
#include "matcher.h"
#include "dataset.h"
#include "dataset/local.h"
#include "validator.h"
#include "types/reftime.h"
#include "types/source.h"
#include "utils/string.h"
#include "utils/sys.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {

static inline Matcher getFilter(const ConfigFile* cfg)
{
    try {
        return Matcher::parse(cfg->value("filter"));
    } catch (std::runtime_error& e) {
        const configfile::Position* fp = cfg->valueInfo("filter");
        if (fp)
        {
            stringstream ss;
            ss << fp->pathname << ":" << fp->lineno << ": " << e.what();
            throw std::runtime_error(ss.str());
        }
        throw;
    }
}

Dispatcher::Dispatcher(const ConfigFile& cfg)
    : m_can_continue(true), m_outbound_failures(0)
{
    // Validate the configuration, and split normal datasets from outbound
    // datasets
    for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
            i != cfg.sectionEnd(); ++i)
    {
        if (i->first == "error" or i->first == "duplicates")
            continue;
        else if (i->second->value("type") == "outbound")
        {
            if (i->second->value("filter").empty())
                throw std::runtime_error("configuration of dataset '"+i->first+"' does not have a 'filter' directive");
            outbounds.push_back(make_pair(i->first, getFilter(i->second)));
        }
        else {
            if (i->second->value("filter").empty())
                throw std::runtime_error("configuration of dataset '"+i->first+"' does not have a 'filter' directive");
            datasets.push_back(make_pair(i->first, getFilter(i->second)));
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


RealDispatcher::RealDispatcher(const ConfigFile& cfg)
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



TestDispatcher::TestDispatcher(const ConfigFile& cfg, std::ostream& out)
    : Dispatcher(cfg), cfg(cfg), out(out)
{
    if (!cfg.section("error"))
        throw std::runtime_error("no [error] dataset found");
}
TestDispatcher::~TestDispatcher() {}

void TestDispatcher::raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    if (batch.empty()) return;
    // TODO: forward drop_cached_data_on_commit
    dataset::Writer::test_acquire(*cfg.section(name), batch, out);
}

void TestDispatcher::dispatch(dataset::WriterBatch& batch, bool drop_cached_data_on_commit)
{
    Dispatcher::dispatch(batch, drop_cached_data_on_commit);
    for (const auto& e: batch)
    {
        out << "Message " << e->md.source() << ": ";
        if (e->dataset_name.empty())
            out << "not imported";
        else
            out << "imported into " << e->dataset_name;
        out << endl;
        out << "  Notes:" << endl;
        for (const auto& note: e->md.notes())
            out << "    " << note.content << endl;
    }
}

void TestDispatcher::flush()
{
}

}
