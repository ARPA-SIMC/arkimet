#include "dispatch.h"
#include "processor.h"
#include "arki/runtime.h"
#include "arki/runtime/config.h"
#include "arki/dispatcher.h"
#include "arki/utils.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "arki/validator.h"
#include "arki/types/source/blob.h"
#include <sys/time.h>
#include <iostream>

using namespace std;
using namespace arki::utils;

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif

namespace arki {
namespace runtime {

DispatchResults::DispatchResults()
{
    gettimeofday(&start_time, NULL);
    timerclear(&end_time);
}

void DispatchResults::end()
{
    gettimeofday(&end_time, NULL);
}

bool DispatchResults::success(bool ignore_duplicates) const
{
    bool success = !(not_imported || in_error_dataset);
    if (ignore_duplicates)
        return success && (successful || duplicates);
    else
        return success && (successful && !duplicates);
}

std::string DispatchResults::summary() const
{
    std::string timeinfo;
    if (timerisset(&end_time))
    {
        struct timeval diff;
        timersub(&end_time, &start_time, &diff);
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!successful && !not_imported && !duplicates && !in_error_dataset)
        return "no data processed" + timeinfo;

    if (!not_imported && !duplicates && !in_error_dataset)
    {
        stringstream ss;
        ss << "everything ok: " << successful << " message";
        if (successful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
    }

    stringstream res;

    if (not_imported)
        res << "serious problems: ";
    else
        res << "some problems: ";

    res << successful << " ok, "
        << duplicates << " duplicates, "
        << in_error_dataset << " in error dataset";

    if (not_imported)
        res << ", " << not_imported << " NOT imported";

    res << timeinfo;

    return res.str();
}


MetadataDispatch::MetadataDispatch(DatasetProcessor& next)
    : next(next)
{
}

MetadataDispatch::~MetadataDispatch()
{
    if (dispatcher)
        delete dispatcher;
}

DispatchResults MetadataDispatch::process(dataset::Reader& ds, const std::string& name)
{
    DispatchResults stats;
    stats.name = name;

    results.clear();

    if (!dir_copyok.empty())
        copyok.reset(new core::File(str::joinpath(dir_copyok, str::basename(name))));
    else
        copyok.reset();

    if (!dir_copyko.empty())
        copyko.reset(new core::File(str::joinpath(dir_copyko, str::basename(name))));
    else
        copyko.reset();

    // Read
    try {
        ds.query_data(Matcher(), [&](unique_ptr<Metadata> md) {
            partial_batch_data_size += md->data_size();
            partial_batch.acquire(move(md));
            if (flush_threshold && partial_batch_data_size > flush_threshold)
                process_partial_batch(name, stats);
            return true;
        });
        if (!partial_batch.empty())
            process_partial_batch(name, stats);
    } catch (std::exception& e) {
        nag::warning("%s: cannot read contents: %s", name.c_str(), e.what());
        next.process(results, name);
        throw;
    }

    // Process the resulting annotated metadata as a dataset
    next.process(results, name);

    stats.end();

    flush();

    return stats;
}

void MetadataDispatch::process_partial_batch(const std::string& name, DispatchResults& stats)
{
    bool drop_cached_data_on_commit = !(copyok || copyko);

    // Dispatch
    auto batch = partial_batch.make_import_batch();
    try {
        dispatcher->dispatch(batch, drop_cached_data_on_commit);
    } catch (std::exception& e) {
        nag::warning("%s: cannot dispatch contents: %s", name.c_str(), e.what());
        partial_batch.move_to(results.inserter_func());
        throw;
    }

    // Evaluate results
    for (auto& e: batch)
    {
        if (e->dataset_name.empty())
        {
            do_copyko(e->md);
            // If dispatching failed, add a big note about it.
            e->md.add_note("WARNING: The data has not been imported in ANY dataset");
            ++stats.not_imported;
        } else if (e->dataset_name == "error") {
            do_copyko(e->md);
            ++stats.in_error_dataset;
        } else if (e->dataset_name == "duplicates") {
            do_copyko(e->md);
            ++stats.duplicates;
        } else if (e->result == dataset::ACQ_OK) {
            do_copyok(e->md);
            ++stats.successful;
        } else {
            do_copyko(e->md);
            // If dispatching failed, add a big note about it.
            e->md.add_note("WARNING: The data failed to be imported into dataset " + e->dataset_name);
            ++stats.not_imported;
        }
        e->md.drop_cached_data();
    }

    // Process the resulting annotated metadata as a dataset
    partial_batch.move_to(results.inserter_func());
    partial_batch_data_size = 0;
}

void MetadataDispatch::do_copyok(Metadata& md)
{
    if (!copyok)
        return;

    if (!copyok->is_open())
        copyok->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyok);
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (!copyko)
        return;

    if (!copyko->is_open())
        copyko->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyko);
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

}
}
