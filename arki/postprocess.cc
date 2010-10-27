/*
 * postprocess - postprocessing of result data
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "postprocess.h"
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/operators.h>
#include <wibble/sys/childprocess.h>
#include <wibble/sys/process.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "config.h"
#include <signal.h>
#include <unistd.h>
#include <cerrno>
#include <iostream>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace wibble;

namespace arki {

namespace postproc {
class Subcommand : public wibble::sys::ChildProcess
{
    vector<string> args;

    virtual int main()
    {
        try {
            //cerr << "RUN args: " << str::join(args.begin(), args.end()) << endl;

            // Build the argument list
            string basename = str::basename(args[0]);
            const char *argv[args.size()+1];
            for (size_t i = 0; i < args.size(); ++i)
            {
                if (i == 0)
                    argv[i] = basename.c_str();
                else
                    argv[i] = args[i].c_str();
            }
            argv[args.size()] = 0;

            // Exec the filter
            //cerr << "RUN " << args[0] << " args: " << str::join(&argv[0], &argv[args.size()]) << endl;
            execv(args[0].c_str(), (char* const*)argv);
            throw wibble::exception::System("running postprocessing filter");
        } catch (std::exception& e) {
            cerr << e.what() << endl;
            return 1;
        }
        return 2;
    }

public:
    Subcommand(const vector<string>& args)
        : args(args)
    {
    }
    ~Subcommand()
    {
    }
};

/**
 * Manage a child process as a filter
 */
struct FilterHandler
{
    /// Subprocess that does the filtering
    wibble::sys::ChildProcess* subproc;
    /// Pipe used to send data to the subprocess
    int infd;
    /// Pipe used to get data back from the subprocess
    int outfd;
    /// Wait timeout: { 0, 0 } for wait forever
    struct timespec timeout;

    FilterHandler(wibble::sys::ChildProcess* subproc)
        : subproc(subproc), infd(-1), outfd(-1)
    {
        timeout.tv_sec = 0;
        timeout.tv_nsec = 0;
    }

    ~FilterHandler()
    {
        if (infd != -1) close(infd);
        if (outfd != -1) close(outfd);
    }

    /// Start the child process, setting up pipes as needed
    void start()
    {
        subproc->forkAndRedirect(&infd, &outfd);
    }

    /// Close pipe to child process, to signal we're done sending data
    void done_with_input()
    {
        if (infd != -1)
        {
            //cerr << "Closing pipe" << endl;
            if (close(infd) < 0)
                throw wibble::exception::System("closing output to filter child process");
            infd = -1;
        }
    }

    /**
     * Wait until the child has data to be read, feeding it data in the
     * meantime.
     *
     * \a buf and \a size will be updated to point to the unwritten bytes.
     * After the function returns, \a size will contain the amount of data
     * still to be written (or 0).
     *
     * @return true if there is data to be read, false if there is no data but
     * all the buffer has been written
     */
    bool wait_for_child_data_while_sending(const void*& buf, size_t& size)
    {
        struct timespec* timeout_param = NULL;
        if (timeout.tv_sec != 0 || timeout.tv_nsec != 0)
            timeout_param = &timeout;

        while (size > 0)
        {
            fd_set infds;
            fd_set outfds;
            FD_ZERO(&infds);
            FD_SET(infd, &infds);
            FD_ZERO(&outfds);
            FD_SET(outfd, &outfds);
            int nfds = max(infd, outfd) + 1;

            int res = pselect(nfds, &outfds, &infds, NULL, timeout_param, NULL);
            if (res < 0)
                throw wibble::exception::System("waiting for activity on filter child process");
            if (res == 0)
                throw wibble::exception::Consistency("timeout waiting for activity on filter child process");

            if (FD_ISSET(infd, &infds))
            {
                ssize_t res = write(infd, buf, size);
                if (res < 0)
                    throw wibble::exception::System("writing to child process");
                buf = (const char*)buf + res;
                size -= res;
            }

            if (FD_ISSET(outfd, &outfds))
                return true;
        }
        return false;
    }

    /// Wait until the child has data to read
    void wait_for_child_data()
    {
        struct timespec* timeout_param = NULL;
        if (timeout.tv_sec != 0 || timeout.tv_nsec != 0)
            timeout_param = &timeout;

        while (true)
        {
            fd_set outfds;
            FD_ZERO(&outfds);
            FD_SET(outfd, &outfds);
            int nfds = outfd + 1;

            int res = pselect(nfds, &outfds, NULL, NULL, timeout_param, NULL);
            if (res < 0)
                throw wibble::exception::System("waiting for activity on filter child process");
            if (res == 0)
                throw wibble::exception::Consistency("timeout waiting for activity on filter child process");
            if (FD_ISSET(outfd, &outfds))
                return;
        }
    }
};

/*
 * One can ignore SIGPIPE (using, for example, the signal system call). In this case, all system calls that would cause SIGPIPE to be sent will return -1 and set errno to EPIPE.
 */
/*
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
*/

}

void Postprocess::init()
{
    // This could have been handled using a default argument, but it fails
    // to compile on at least fc8
    map<string, string> cfg;
    init(cfg);
}

void Postprocess::init(const map<string, string>& cfg)
{
    using namespace wibble::operators;
    Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
    // Build the set of allowed postprocessors
    set<string> allowed;
    map<string, string>::const_iterator i = cfg.find("postprocess");
    if (i != cfg.end())
    {
        string pp = i->second;
        for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Parse command into its components
    vector<string> args;
    for (Splitter::const_iterator j = sp.begin(m_command); j != sp.end(); ++j)
        args.push_back(*j);
    //cerr << "Split \"" << m_command << "\" into: " << str::join(args.begin(), args.end(), ", ") << endl;

    // Validate the command
    if (args.empty())
        throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command is empty");
    if (!cfg.empty() && allowed.find(args[0]) == allowed.end())
    {
        throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command " + m_command + " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(allowed.begin(), allowed.end()) + ")");
    }

    // Expand args[0] to the full pathname and check that the program exists

    // Build a list of argv0 candidates
    vector<string> cand_argv0;
    char* env_ppdir = getenv("ARKI_POSTPROC");
    if (env_ppdir)
        cand_argv0.push_back(str::joinpath(env_ppdir, args[0]));
    cand_argv0.push_back(str::joinpath(POSTPROC_DIR, args[0]));

    // Get the first good one from the list
    string argv0;
    for (vector<string>::const_iterator i = cand_argv0.begin();
            i != cand_argv0.end(); ++i)
        if (sys::fs::access(*i, X_OK))
            argv0 = *i;

    if (argv0.empty())
        throw wibble::exception::Consistency("running postprocessing filter", "postprocess command \"" + m_command + "\" does not exists or it is not executable; tried: " + str::join(cand_argv0.begin(), cand_argv0.end()));
    args[0] = argv0;

    // Spawn the command
    m_child = new postproc::Subcommand(args);
    m_handler = new postproc::FilterHandler(m_child);
    m_handler->start();
}

Postprocess::Postprocess(const std::string& command, int outfd)
    : m_child(0), m_handler(0), m_command(command), m_nextfd(outfd), m_out(NULL), data_start_hook(NULL)
{
    init();
}

Postprocess::Postprocess(const std::string& command, int outfd, const map<string, string>& cfg)
    : m_child(0), m_handler(0), m_command(command), m_nextfd(outfd), m_out(NULL), data_start_hook(NULL)
{
    init(cfg);
}

Postprocess::Postprocess(const std::string& command, std::ostream& out)
    : m_child(0), m_handler(0), m_command(command), m_nextfd(-1), m_out(&out), data_start_hook(NULL)
{
    if (stream::PosixBuf* ps = dynamic_cast<stream::PosixBuf*>(out.rdbuf()))
        m_nextfd = ps->fd();

    init();
}

Postprocess::Postprocess(const std::string& command, std::ostream& out, const map<string, string>& cfg)
    : m_child(0), m_handler(0), m_command(command), m_nextfd(-1), m_out(&out), data_start_hook(NULL)
{
    if (stream::PosixBuf* ps = dynamic_cast<stream::PosixBuf*>(out.rdbuf()))
        m_nextfd = ps->fd();

    init(cfg);
}

Postprocess::~Postprocess()
{
    // If the child still exists, it means that we have not flushed: be
    // aggressive here to cleanup after whatever error condition has happened
    // in the caller
    if (m_child)
    {
        m_child->kill(SIGTERM);
        m_child->wait();
        delete m_child;
    }
    if (m_handler)
        delete m_handler;
}

void Postprocess::set_data_start_hook(metadata::Hook* hook)
{
    data_start_hook = hook;
}

bool Postprocess::read_and_pass_on()
{
    // After we've seen some data, we can leave it up to splice
    if (!data_start_hook)
    {
        if (m_nextfd != -1)
        {
            // Try splice
            ssize_t res = splice(m_handler->outfd, NULL, m_nextfd, NULL, 4096*2, SPLICE_F_MORE);
            if (res >= 0)
                return res > 0;
            if (errno != EINVAL)
                throw wibble::exception::System("splicing data from child postprocessor to destination");
            // Else pass it on to the traditional method
        }
    }
    // Else read and write

    // Read data from child
    char buf[4096*2];
    ssize_t res = read(m_handler->outfd, buf, 4096*2);
    if (res < 0)
        throw wibble::exception::System("reading from child postprocessor");
    if (res == 0)
        return false;

    if (data_start_hook)
    {
        // Fire hook
        (*data_start_hook)();
        // Only once
        data_start_hook = 0;
    }

    // Pass it on
    if (m_out == NULL)
    {
        size_t pos = 0;
        while (pos < (size_t)res)
        {
            ssize_t wres = write(m_nextfd, buf+pos, res-pos);
            if (wres < 0)
                throw wibble::exception::System("writing to destination file descriptor");
            pos += wres;
        }
    } else {
        m_out->write(buf, res);
        if (m_out->bad())
            throw wibble::exception::System("writing to destination stream");
    }

    // There is still data to read
    return true;
}

/// Write the given buffer to the child, pumping out the child output in the meantime
bool Postprocess::pump(const void* buf, size_t size)
{
    while (size > 0)
    {
        if (m_handler->wait_for_child_data_while_sending(buf, size))
        {
            if (!read_and_pass_on())
                return false;
        }
    }
    return true;
}

/// Just pump the child output out
void Postprocess::pump()
{
    while (true)
    {
        m_handler->wait_for_child_data();
        if (!read_and_pass_on())
            return;
    }
}

bool Postprocess::operator()(Metadata& md)
{
    if (m_handler->infd == -1)
        return false;

    // Ignore sigpipe when writing to child, so we can raise an exception
    // instead of dying with the signal
    // TODO: handle instead of ignoring, otherwise write will just wait and hang
    // Sigignore sigignore(SIGPIPE);

    string encoded = md.encode();
    pump(encoded.data(), encoded.size());

    wibble::sys::Buffer data = md.getData();
    pump(data.data(), data.size());

    return true;
}

void Postprocess::flush()
{
    m_handler->done_with_input();
    pump();

    if (m_child)
    {
        //cerr << "Waiting for child" << endl;
        int res = m_child->wait();
        //cerr << "Waited and got " << res << endl;
        delete m_child;
        m_child = 0;
        if (res)
            throw wibble::exception::Consistency("running postprocessing filter", "postprocess command \"" + m_command + "\" " + sys::process::formatStatus(res));
    }
}

}
// vim:set ts=4 sw=4:
