#ifndef ARKI_PYTHON_ARKISCAN_DISPATCH_H
#define ARKI_PYTHON_ARKISCAN_DISPATCH_H

#include <arki/core/cfg.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/memory.h>
#include "results.h"
#include <filesystem>
#include <vector>

namespace arki {
class Dispatcher;

namespace python {

namespace cmdline {
class DatasetProcessor;
}

namespace arki_scan {

/// Dispatch metadata
class MetadataDispatch
{
public:
    std::shared_ptr<arki::dataset::Pool> pool;
    core::cfg::Sections cfg;
    Dispatcher* dispatcher = nullptr;
    std::shared_ptr<dataset::memory::Dataset> partial_batch;
    size_t flush_threshold = 128 * 1024 * 1024;
    size_t partial_batch_data_size = 0;
    std::shared_ptr<dataset::memory::Dataset> results;
    cmdline::DatasetProcessor& next;

    /// Directory where we store copyok files
    std::filesystem::path dir_copyok;

    /// Directory where we store copyko files
    std::filesystem::path dir_copyko;

    /// File to which we send data that was successfully imported
    std::shared_ptr<core::File> copyok;
    std::unique_ptr<StreamOutput> copyok_stream;

    /// File to which we send data that was not successfully imported
    std::shared_ptr<core::File> copyko;
    std::unique_ptr<StreamOutput> copyko_stream;


    MetadataDispatch(std::shared_ptr<arki::dataset::Pool> pool, cmdline::DatasetProcessor& next);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    DispatchResults process(dataset::Reader& ds, const std::filesystem::path& name);

    // Flush all imports done so far
    void flush();

protected:
    void process_partial_batch(const std::filesystem::path& name, DispatchResults& stats);
    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};


}
}
}
#endif
