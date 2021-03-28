#include "config.h"
#include "postprocess.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/utils/process.h"
#include "arki/utils/string.h"
#include "arki/utils/subprocess.h"
#include "arki/utils/regexp.h"
#include "arki/runtime.h"
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
#include <set>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace {

/*
 * One can ignore SIGPIPE (using, for example, the signal system call). In this
 * case, all system calls that would cause SIGPIPE to be sent will return -1
 * and set errno to EPIPE.
 */
struct Sigignore
{
    int signum;
    sighandler_t oldsig;

    Sigignore(int signum) : signum(signum)
    {
        oldsig = signal(signum, SIG_IGN);
    }
    ~Sigignore()
    {
        signal(signum, oldsig);
    }
};

}


namespace arki {
namespace metadata {
namespace postproc {

class Child : public utils::IODispatcher
{
public:
    /// Subcommand with the child to run
    subprocess::Popen cmd;

    /**
     * StreamOutput used to send data from the subprocess to the output stream.
     */
    StreamOutput* m_stream = nullptr;

    /// Stream where child stderr is sent
    std::ostream* m_err = 0;

    Child() : utils::IODispatcher(cmd)
    {
        cmd.set_stdin(subprocess::Redirect::PIPE);
        cmd.set_stdout(subprocess::Redirect::PIPE);
        cmd.set_stderr(subprocess::Redirect::PIPE);
    }

    void read_stdout() override
    {
        // Stream directly out of a pipe
        size_t res = m_stream->send_from_pipe(subproc.get_stdout());
        if (res == 0)
            subproc.close_stdout();
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

Postprocess::Postprocess(const std::string& command)
    : m_command(command)
{
    m_child = new postproc::Child();
    m_child->m_err = &m_errors;

    // Parse command into its components
    Splitter sp("[[:space:]]+", REG_EXTENDED);
    for (Splitter::const_iterator j = sp.begin(m_command); j != sp.end(); ++j)
        m_child->cmd.args.push_back(*j);
}

Postprocess::~Postprocess()
{
    // If the child still exists, it means that we have not flushed: be
    // aggressive here to cleanup after whatever error condition has happened
    // in the caller
    if (m_child)
    {
        m_child->terminate();
        delete m_child;
    }
}

void Postprocess::set_output(StreamOutput& out)
{
    m_child->m_stream = &out;
}

void Postprocess::set_error(std::ostream& err)
{
    m_child->m_err = &err;
}

void Postprocess::validate(const core::cfg::Section& cfg)
{
    // Build the set of allowed postprocessors
    set<string> allowed;
    bool has_cfg = cfg.has("postprocess");
    if (has_cfg)
    {
        Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
        string pp = cfg.value("postprocess");
        for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Validate the command
    if (m_child->cmd.args.empty())
        throw std::runtime_error("cannot initialize postprocessing filter: postprocess command is empty");
    string scriptname = str::basename(m_child->cmd.args[0]);
    if (has_cfg && allowed.find(scriptname) == allowed.end())
    {
        stringstream ss;
        ss << "cannot initialize postprocessing filter: postprocess command "
           << m_command
           << " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(", ", allowed.begin(), allowed.end())
           << ")";
        throw std::runtime_error(ss.str());
    }
}

void Postprocess::start()
{
    // Expand args[0] to the full pathname and check that the program exists
    m_child->cmd.args[0] = Config::get().dir_postproc.find_file(m_child->cmd.args[0], true);

    // Spawn the command
    m_child->subproc.fork();

    // set the stdin/stdout/stderr fds to nonblocking
    sys::FileDescriptor child_stdin(m_child->cmd.get_stdin());
    if (child_stdin != -1)
        child_stdin.setfl(child_stdin.getfl() | O_NONBLOCK);
    sys::FileDescriptor child_stdout(m_child->cmd.get_stdout());
    if (child_stdout != -1)
        child_stdout.setfl(child_stdout.getfl() | O_NONBLOCK);
    sys::FileDescriptor child_stderr(m_child->cmd.get_stderr());
    if (child_stderr != -1)
        child_stderr.setfl(child_stderr.getfl() | O_NONBLOCK);
}

bool Postprocess::process(std::shared_ptr<Metadata> md)
{
    if (m_child->subproc.get_stdin() == -1)
        return false;

    // Make the data inline, so that the reader on the other side, knows that
    // we are sending that as well
    md->makeInline();

    auto encoded = md->encodeBinary();
    if (m_child->send(encoded) < encoded.size())
        return false;

    const auto& data = md->get_data();
    const auto buf = data.read();
    if (m_child->send(buf.data(), buf.size()) < buf.size())
        return false;

    return true;
}

void Postprocess::flush()
{
    m_child->flush();

    m_child->subproc.wait();
    int res = m_child->subproc.raw_returncode();
    delete m_child;
    m_child = 0;
    if (res)
    {
        string msg = "cannot run postprocessing filter: postprocess command \"" + m_command + "\" " + subprocess::Child::format_raw_returncode(res);
        if (!m_errors.str().empty())
            msg += "; stderr: " + str::strip(m_errors.str());
        throw std::runtime_error(msg);
    }
}

}
}
