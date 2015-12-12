#include "config.h"

#include "postprocess.h"
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/utils/process.h>
#include <arki/runtime/config.h>
#include <arki/utils/string.h>
#include <arki/wibble/regexp.h>
#include <arki/wibble/sys/childprocess.h>
#include <arki/wibble/sys/process.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <unistd.h>
#include <cerrno>
#include <iostream>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace arki::utils;

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
    utils::Subcommand cmd;
    /// Non-null if we should notify the hook as soon as some data arrives from the processor
    std::function<void()> data_start_hook;
    /**
     * Pipe used to send data from the subprocess to the output stream. It can
     * be -1 if the output stream does not provide a file descriptor to write
     * to
     */
    int m_nextfd;
    /**
     * Output stream we eventually send data to.
     *
     * It can be NULL if we have only been given a file descriptor to send data
     * to.
     */
    std::ostream* m_out;
    /// Stream where child stderr is sent
    std::ostream* m_err;

    Child()
        : utils::IODispatcher(cmd),
          m_nextfd(-1), m_out(0), m_err(0)
    {
    }

    virtual void read_stdout()
    {
#if defined(__linux__)
#ifdef HAVE_SPLICE
        // After we've seen some data, we can leave it up to splice
        if (!data_start_hook)
        {
            if (m_nextfd != -1)
            {
                // Try splice
                ssize_t res = splice(outfd, NULL, m_nextfd, NULL, 4096*2, SPLICE_F_MORE);
                if (res >= 0)
                {
                    if (res == 0)
                        close_outfd();
                    return;
                }
                if (errno != EINVAL)
                    throw wibble::exception::System("splicing data from child postprocessor to destination");
                // Else pass it on to the traditional method
            }
        }
#endif
#endif
        // Else read and write

        // Read data from child
        char buf[4096*2];
        ssize_t res = read(outfd, buf, 4096*2);
        if (res < 0)
            throw wibble::exception::System("reading from child postprocessor");
        if (res == 0)
        {
            close_outfd();
            return;
        }
        if (data_start_hook)
        {
            // Fire hook
            data_start_hook();
            // Only once
            data_start_hook = nullptr;
        }

        // Pass it on
        if (m_nextfd != -1)
        {
            Sigignore ignpipe(SIGPIPE);
            size_t pos = 0;
            while (pos < (size_t)res)
            {
                ssize_t wres = write(m_nextfd, buf+pos, res-pos);
                if (wres < 0)
                {
                    if (errno == EPIPE)
                    {
                        close(m_nextfd);
                        m_nextfd = -1;
                    } else
                        throw wibble::exception::System("writing to destination file descriptor");
                }
                pos += wres;
            }
        }
        else if (m_out != NULL)
        {
            m_out->write(buf, res);
            if (m_out->bad())
                throw wibble::exception::System("writing to destination stream");
            if (m_out->eof())
                m_out = NULL;
        }
    }

    virtual void read_stderr()
    {
        if (m_err)
        {
            if (!fd_to_stream(errfd, *m_err))
            {
                close_errfd();
            }
        } else {
            if (!discard_fd(errfd))
            {
                close_errfd();
            }
        }
    }

    void terminate()
    {
        if (cmd.pid() != -1)
        {
            cmd.kill(SIGTERM);
            cmd.wait();
        }
    }
};

}

Postprocess::Postprocess(const std::string& command)
    : m_child(0), m_command(command)
{
    m_child = new postproc::Child();
    m_child->m_err = &m_errors;

    // Parse command into its components
    wibble::Splitter sp("[[:space:]]+", REG_EXTENDED);
    for (wibble::Splitter::const_iterator j = sp.begin(m_command); j != sp.end(); ++j)
        m_child->cmd.args.push_back(*j);
    //cerr << "Split \"" << m_command << "\" into: " << str::join(m_child->cmd.args.begin(), m_child->cmd.args.end(), ", ") << endl;
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

void Postprocess::set_output(int outfd)
{
    m_child->m_nextfd = outfd;
}

void Postprocess::set_error(std::ostream& err)
{
    m_child->m_err = &err;
}

void Postprocess::set_data_start_hook(std::function<void()> hook)
{
    m_child->data_start_hook = hook;
}

void Postprocess::validate(const map<string, string>& cfg)
{
    // Build the set of allowed postprocessors
    set<string> allowed;
    map<string, string>::const_iterator i = cfg.find("postprocess");
    if (i != cfg.end())
    {
        wibble::Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
        string pp = i->second;
        for (wibble::Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Validate the command
    if (m_child->cmd.args.empty())
        throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command is empty");
    string scriptname = str::basename(m_child->cmd.args[0]);
    if (i != cfg.end() && allowed.find(scriptname) == allowed.end())
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
    m_child->cmd.args[0] = runtime::Config::get().dir_postproc.find_file(m_child->cmd.args[0], true);

    // Spawn the command
    m_child->start();
}

bool Postprocess::process(unique_ptr<Metadata>&& md)
{
    if (m_child->infd == -1)
        return false;

    // Make the data inline, so that the reader on the other side, knows that
    // we are sending that as well
    md->makeInline();

    string encoded = md->encodeBinary();
    if (m_child->send(encoded) < encoded.size())
        return false;

    const auto& data = md->getData();
    if (m_child->send(data.data(), data.size()) < data.size())
        return false;

    return true;
}

void Postprocess::flush()
{
    m_child->flush();

    //cerr << "Waiting for child" << endl;
    int res = m_child->cmd.wait();
    //cerr << "Waited and got " << res << endl;
    delete m_child;
    m_child = 0;
    if (res)
    {
        string msg = "postprocess command \"" + m_command + "\" " + wibble::sys::process::formatStatus(res);
        if (!m_errors.str().empty())
            msg += "; stderr: " + str::strip(m_errors.str());
        throw wibble::exception::Consistency("running postprocessing filter", msg);
    }
}

}
