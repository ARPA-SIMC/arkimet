#ifndef ARKI_STREAM_FILTER_H
#define ARKI_STREAM_FILTER_H

#include "arki/stream.h"
#include "arki/utils/process.h"
#include "arki/utils/subprocess.h"

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

    /// Accumulated stream result
    size_t size_stdin = 0;
    size_t size_stdout = 0;

    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream errors;


    FilterProcess(const std::vector<std::string>& args);

    void start();
    void stop();

    void terminate()
    {
        if (cmd.started())
        {
            cmd.terminate();
            cmd.wait();
        }
    }

    /**
     * Raise std::runtime_error if the filter exited with a nonzero exit status
     */
    void check_for_errors();
};

}
}

#endif
