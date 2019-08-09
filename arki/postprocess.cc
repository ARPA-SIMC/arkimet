#include "config.h"
#include "postprocess.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/utils/process.h"
#include "arki/utils/string.h"
#include "arki/utils/subprocess.h"
#include "arki/utils/regexp.h"
#include "arki/runtime.h"
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <unistd.h>
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

namespace postproc {

struct Child : public utils::IODispatcher
{
    /// Subcommand with the child to run
    subprocess::Popen cmd;

    /// Non-null if we should notify the hook as soon as some data arrives from the processor
    std::function<void(NamedFileDescriptor&)> data_start_hook;

    /**
     * Pipe used to send data from the subprocess to the output stream. It can
     * be -1 if the output stream does not provide a file descriptor to write
     * to
     */
    NamedFileDescriptor* m_nextfd = nullptr;

    /// Alternative to m_nextfd when we are not writing to a file descriptor
    AbstractOutputFile* m_outfile = nullptr;

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
        if (m_nextfd)
            read_stdout_fd();
        else
            read_stdout_aof();
    }

    void read_stdout_aof()
    {
        // Read data from child
        char buf[4096*2];
        ssize_t res = read(subproc.get_stdout(), buf, 4096*2);
        if (res < 0)
            throw_system_error("reading from child postprocessor");
        if (res == 0)
        {
            subproc.close_stdout();
            return;
        }

        // Pass it on
        Sigignore ignpipe(SIGPIPE);
        m_outfile->write(buf, res);
    }

    void read_stdout_fd()
    {
#if defined(__linux__)
#ifdef HAVE_SPLICE
        // After we've seen some data, we can leave it up to splice
        if (!data_start_hook)
        {
            if (m_nextfd)
            {
                // Try splice
                ssize_t res = splice(subproc.get_stdout(), NULL, *m_nextfd, NULL, 4096*2, SPLICE_F_MORE);
                if (res >= 0)
                {
                    if (res == 0)
                        subproc.close_stdout();
                    return;
                }
                if (errno != EINVAL)
                    throw_system_error("cannot splice data from child postprocessor to destination");
                // Else pass it on to the traditional method
            }
        }
#endif
#endif
        // Else read and write

        // Read data from child
        char buf[4096*2];
        ssize_t res = read(subproc.get_stdout(), buf, 4096*2);
        if (res < 0)
            throw_system_error("reading from child postprocessor");
        if (res == 0)
        {
            subproc.close_stdout();
            return;
        }
        if (data_start_hook)
        {
            // Fire hook
            data_start_hook(*m_nextfd);
            // Only once
            data_start_hook = nullptr;
        }

        // Pass it on
        Sigignore ignpipe(SIGPIPE);
        size_t pos = 0;
        while (m_nextfd && pos < (size_t)res)
        {
            ssize_t wres = write(*m_nextfd, buf+pos, res-pos);
            if (wres < 0)
            {
                if (errno == EPIPE)
                {
                    m_nextfd = nullptr;
                } else
                    throw_system_error("writing to destination file descriptor");
            }
            pos += wres;
        }
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

void Postprocess::set_output(NamedFileDescriptor& outfd)
{
    m_child->m_nextfd = &outfd;
}

void Postprocess::set_output(core::AbstractOutputFile& outfd)
{
    m_child->m_outfile = &outfd;
}

void Postprocess::set_error(std::ostream& err)
{
    m_child->m_err = &err;
}

void Postprocess::set_data_start_hook(std::function<void(NamedFileDescriptor&)> hook)
{
    m_child->data_start_hook = hook;
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
