#ifndef ARKI_PYTHON_ARKISCAN_RESULTS_H
#define ARKI_PYTHON_ARKISCAN_RESULTS_H

#include <filesystem>
#include <string>

namespace arki {
namespace python {
namespace arki_scan {

struct DispatchResults
{
    /// Name of source that was imported
    std::filesystem::path name;

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

}
}
}

#endif
