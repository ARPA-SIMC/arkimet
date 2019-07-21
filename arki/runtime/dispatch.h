#ifndef ARKI_RUNTIME_DISPATCH_H
#define ARKI_RUNTIME_DISPATCH_H

#include <arki/core/cfg.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/memory.h>
#include <arki/runtime/module.h>
#include <string>
#include <vector>

namespace arki {
class Dispatcher;

namespace runtime {
class DatasetProcessor;

struct DispatchResults
{
    /// Name of source that was imported
    std::string name;

    // Used for timings. Read with gettimeofday at the beginning of a task,
    // and summary_so_far will report the elapsed time
    struct timeval start_time;

    // Used for timings. Read with gettimeofday at the beginning of a task,
    // and summary_so_far will report the elapsed time
    struct timeval end_time;

    /// Count of metadata imported successfully in the destination dataset
    unsigned successful = 0;

    /// Count of metadata imported in the error dataset because it had already
    /// been imported
    unsigned duplicates = 0;

    /// Count of metadata imported in the error dataset
    unsigned in_error_dataset = 0;

    /// Count of metadata not imported at all
    unsigned not_imported = 0;


    DispatchResults();

    /// Notify the end of processing for this source
    void end();

    /// Format a summary
    std::string summary() const;

    /// Check if dispatching was successful
    bool success(bool ignore_duplicates) const;
};

/// Dispatch metadata
struct MetadataDispatch
{
    core::cfg::Sections cfg;
    Dispatcher* dispatcher = nullptr;
    dataset::Memory partial_batch;
    size_t flush_threshold = 128 * 1024 * 1024;
    size_t partial_batch_data_size = 0;
    dataset::Memory results;
    DatasetProcessor& next;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<core::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<core::File> copyko;


    MetadataDispatch(DatasetProcessor& next);
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
#endif
