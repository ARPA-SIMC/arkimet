#ifndef ARKI_PYTHON_ARKISCAN_DISPATCH_H
#define ARKI_PYTHON_ARKISCAN_DISPATCH_H

#include <arki/core/cfg.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/memory.h>
#include "results.h"
#include <string>
#include <vector>

namespace arki {
class Dispatcher;

namespace runtime {
class DatasetProcessor;
}

namespace python {

namespace arki_scan {


/// Dispatch metadata
struct MetadataDispatch
{
    core::cfg::Sections cfg;
    Dispatcher* dispatcher = nullptr;
    dataset::Memory partial_batch;
    size_t flush_threshold = 128 * 1024 * 1024;
    size_t partial_batch_data_size = 0;
    dataset::Memory results;
    runtime::DatasetProcessor& next;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<core::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<core::File> copyko;


    MetadataDispatch(runtime::DatasetProcessor& next);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    DispatchResults process(dataset::Reader& ds, const std::string& name);

    // Flush all imports done so far
    void flush();

protected:
    void process_partial_batch(const std::string& name, DispatchResults& stats);
    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};


}
}
}
#endif
