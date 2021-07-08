#include "arki/stream.h"
#include "arki/utils/process.h"
#include "arki/utils/subprocess.h"

namespace arki {
namespace stream {

class FilterProcess : public utils::IODispatcher
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
    stream::SendResult stream_result;

    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream errors;


    FilterProcess(const std::vector<std::string>& args);

    void start();

    void read_stdout() override
    {
        // Stream directly out of a pipe
        stream::SendResult res = m_stream->send_from_pipe(subproc.get_stdout());
        stream_result.sent += res.sent;
        if (res.flags & stream::SendResult::SEND_PIPE_EOF_SOURCE)
            subproc.close_stdout();
        else if (res.flags & stream::SendResult::SEND_PIPE_EOF_DEST)
        {
            subproc.close_stdout();
            stream_result.flags |= stream::SendResult::SEND_PIPE_EOF_DEST;
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
