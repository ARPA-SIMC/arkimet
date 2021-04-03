#include "dispatch.h"
#include "python/cmdline/processor.h"
#include "arki/dispatcher.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/stream.h"
#include "arki/nag.h"
#include "arki/metadata.h"
#include "arki/metadata/validator.h"
#include "arki/types/source/blob.h"
#include "arki/dataset/query.h"
#include "arki/scan.h"
#include "python/dataset.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace python {
namespace arki_scan {

MetadataDispatch::MetadataDispatch(std::shared_ptr<arki::dataset::Pool> pool, cmdline::DatasetProcessor& next)
    : pool(pool),
      partial_batch(std::make_shared<arki::dataset::memory::Dataset>(pool->session())),
      results(std::make_shared<arki::dataset::memory::Dataset>(pool->session())), next(next)
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

    results->clear();

    if (!dir_copyok.empty())
    {
        copyok = std::make_shared<core::File>(str::joinpath(dir_copyok, str::basename(name)));
        // We are writing to a file, not a pipe, so we do not need a timeout
        copyok_stream = StreamOutput::create(copyok);
    }
    else
    {
        copyok.reset();
        copyok_stream.reset();
    }

    if (!dir_copyko.empty())
    {
        copyko = std::make_shared<core::File>(str::joinpath(dir_copyko, str::basename(name)));
        // We are writing to a file, not a pipe, so we do not need a timeout
        copyko_stream = StreamOutput::create(copyko);
    }
    else
    {
        copyko.reset();
        copyko_stream.reset();
    }

    // Read
    try {
        ds.query_data(Matcher(), [&](std::shared_ptr<Metadata> md) {
            auto scanner = scan::Scanner::get_scanner(md->source().format);
            scanner->normalize_before_dispatch(*md);
            // TODO: preprocess here, leave untouched or return inline
            partial_batch_data_size += md->data_size();
            partial_batch->acquire(move(md));
            if (flush_threshold && partial_batch_data_size > flush_threshold)
                process_partial_batch(name, stats);
            return true;
        });
        if (!partial_batch->empty())
            process_partial_batch(name, stats);
    } catch (std::exception& e) {
        nag::warning("%s: cannot read contents: %s", name.c_str(), e.what());
        next.process(*results->create_reader(), name);
        throw;
    }

    // Process the resulting annotated metadata as a dataset
    next.process(*results->create_reader(), name);

    stats.end();

    flush();

    return stats;
}

void MetadataDispatch::process_partial_batch(const std::string& name, DispatchResults& stats)
{
    bool drop_cached_data_on_commit = !(copyok || copyko);

    // Dispatch
    auto batch = partial_batch->make_import_batch();
    try {
        dispatcher->dispatch(batch, drop_cached_data_on_commit);
    } catch (std::exception& e) {
        nag::warning("%s: cannot dispatch contents: %s", name.c_str(), e.what());
        partial_batch->move_to(results->inserter_func());
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
    partial_batch->move_to(results->inserter_func());
    partial_batch_data_size = 0;
}

void MetadataDispatch::do_copyok(Metadata& md)
{
    if (!copyok)
        return;

    if (!copyok->is_open())
        copyok->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyok_stream);
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (!copyko)
        return;

    if (!copyko->is_open())
        copyko->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyko_stream);
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

}
}
}
