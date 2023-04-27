#ifndef ARKI_STREAM_FILTER_H
#define ARKI_STREAM_FILTER_H

#include "arki/stream.h"
#include "arki/utils/subprocess.h"
#include <sstream>

namespace arki {
namespace stream {

class FilterProcess
{
public:
    /// Subcommand with the child to run
    utils::subprocess::Popen cmd;

    /**
     * StreamOutput used to send data from the subprocess to the output stream.
     */
    StreamOutput* m_stream = nullptr;

    /**
     * Timeout to use for blocking functions.
     *
     * -1 means infinite, 0 means do not wait
     */
    int timeout_ms;

    /// Accumulated stream result
    size_t size_stdin = 0;
    size_t size_stdout = 0;

    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream errors;


    FilterProcess(const std::vector<std::string>& args, int timeout_ms = -1);

    void start();
    void stop();
    void terminate();

    /**
     * Raise std::runtime_error if the filter exited with a nonzero exit status
     */
    void check_for_errors();
};

}
}

#endif
