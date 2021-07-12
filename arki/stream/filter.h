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

    /// Stream where child stderr is sent
    std::ostream* m_err = 0;

    /// Accumulated stream result
    size_t size_stdin = 0;
    size_t size_stdout = 0;

    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream errors;


    FilterProcess(const std::vector<std::string>& args);

    void start();
    void stop();

#if 0
    void read_stdout() override
    {
        // Stream directly out of a pipe
        size_t sent;
        stream::SendResult res;
        std::tie(sent, res) = m_stream->send_from_pipe(subproc.get_stdout());
        size_stdout += sent;
        if (res.flags & stream::SendResult::SEND_PIPE_EOF_SOURCE)
            subproc.close_stdout();
        else if (res.flags & stream::SendResult::SEND_PIPE_EOF_DEST)
        {
            subproc.close_stdout();
            // TODO stream_result.flags |= stream::SendResult::SEND_PIPE_EOF_DEST;
        }
        return;
    }

    void read_stderr() override
    {
        if (m_err)
        {
            if (!fd_to_stream(subproc.get_stderr(), *m_err))
            {
                subproc.close_stderr();
            }
        } else {
            if (!discard_fd(subproc.get_stderr()))
            {
                subproc.close_stderr();
            }
        }
    }
#endif

    void terminate()
    {
        if (cmd.started())
        {
            cmd.terminate();
            cmd.wait();
        }
    }
};

}
}

#endif
