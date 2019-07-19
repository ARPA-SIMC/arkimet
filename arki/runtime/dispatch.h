#ifndef ARKI_RUNTIME_DISPATCH_H
#define ARKI_RUNTIME_DISPATCH_H

#include <arki/core/cfg.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/memory.h>
#include <arki/runtime/module.h>
#include <arki/utils/commandline/parser.h>
#include <string>
#include <vector>

namespace arki {
class Dispatcher;

namespace runtime {
class ScanCommandLine;
class DatasetProcessor;

struct DispatchOptions : public Module
{
    utils::commandline::OptionGroup* dispatchOpts = nullptr;
    utils::commandline::StringOption* moveok = nullptr;
    utils::commandline::StringOption* moveko = nullptr;
    utils::commandline::StringOption* movework = nullptr;
    utils::commandline::StringOption* copyok = nullptr;
    utils::commandline::StringOption* copyko = nullptr;
    utils::commandline::BoolOption* ignore_duplicates = nullptr;
    utils::commandline::StringOption* validate = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* dispatch = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* testdispatch = nullptr;
    utils::commandline::BoolOption* status = nullptr;
    utils::commandline::StringOption* flush_threshold = nullptr;

    DispatchOptions(ScanCommandLine& args);
    DispatchOptions(const DispatchOptions&) = delete;
    DispatchOptions(DispatchOptions&&) = delete;
    DispatchOptions& operator=(const DispatchOptions&) = delete;
    DispatchOptions& operator=(DispatchOptions&&) = delete;

    bool handle_parsed_options() override;

    bool dispatch_requested() const;
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
    bool ignore_duplicates = false;
    bool reportStatus = false;

    // Used for timings. Read with gettimeofday at the beginning of a task,
    // and summary_so_far will report the elapsed time
    struct timeval startTime;

    // Incremented when a metadata is imported in the destination dataset.
    // Feel free to reset it to 0 anytime.
    int countSuccessful = 0;

    // Incremented when a metadata is imported in the error dataset because it
    // had already been imported.  Feel free to reset it to 0 anytime.
    int countDuplicates = 0;

    // Incremented when a metadata is imported in the error dataset.  Feel free
    // to reset it to 0 anytime.
    int countInErrorDataset = 0;

    // Incremented when a metadata is not imported at all.  Feel free to reset
    // it to 0 anytime.
    int countNotImported = 0;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<core::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<core::File> copyko;


    MetadataDispatch(DatasetProcessor& next);
    MetadataDispatch(const DispatchOptions& args, DatasetProcessor& next);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    bool process(dataset::Reader& ds, const std::string& name);

    // Flush all imports done so far
    void flush();

    // Format a summary of the import statistics so far
    std::string summary_so_far() const;

    // Set startTime to the current time
    void setStartTime();

protected:
    void process_partial_batch(const std::string& name);
    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};


}
}
#endif
