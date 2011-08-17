/*
 * postprocess - postprocessing of result data
 *
 * Copyright (C) 2008--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include "process.h"
#include <wibble/sys/childprocess.h>
#include <wibble/sys/process.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
#include <iostream>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace wibble;

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
namespace utils {

Subcommand::Subcommand()
{
}

Subcommand::Subcommand(const vector<string>& args)
    : args(args)
{
}

Subcommand::~Subcommand()
{
}

int Subcommand::main()
{
    try {
        //cerr << "RUN args: " << str::join(args.begin(), args.end()) << endl;

        // Build the argument list
        //string basename = str::basename(args[0]);
        const char *argv[args.size()+1];
        for (size_t i = 0; i < args.size(); ++i)
        {
            /*
            if (i == 0)
                argv[i] = basename.c_str();
            else
            */
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


IODispatcher::IODispatcher(wibble::sys::ChildProcess& subproc)
    : subproc(subproc), infd(-1), outfd(-1), errfd(-1)
{
    conf_timeout.tv_sec = 0;
    conf_timeout.tv_nsec = 0;
}

IODispatcher::~IODispatcher()
{
    if (infd != -1) close(infd);
    if (outfd != -1) close(outfd);
    if (errfd != -1) close(errfd);
}

void IODispatcher::start(bool do_stdin, bool do_stdout, bool do_stderr)
{
    subproc.forkAndRedirect(
            do_stdin ? &infd : NULL,
            do_stdout ? &outfd : NULL,
            do_stderr ? &errfd : NULL);
}

void IODispatcher::close_infd()
{
    if (infd != -1)
    {
        //cerr << "Closing pipe" << endl;
        if (close(infd) < 0)
            throw wibble::exception::System("closing output to filter child process");
        infd = -1;
    }
}

void IODispatcher::close_outfd()
{
    if (outfd != -1)
    {
        //cerr << "Closing pipe" << endl;
        if (close(outfd) < 0)
            throw wibble::exception::System("closing input from child standard output");
        outfd = -1;
    }
}

void IODispatcher::close_errfd()
{
    if (errfd != -1)
    {
        //cerr << "Closing pipe" << endl;
        if (close(errfd) < 0)
            throw wibble::exception::System("closing input from child standard error");
        errfd = -1;
    }
}

bool IODispatcher::fd_to_stream(int in, std::ostream& out)
{
    // Read data from child
    char buf[4096*2];
    ssize_t res = read(in, buf, 4096*2);
    if (res < 0)
        throw wibble::exception::System("reading from child stderr");
    if (res == 0)
        return false;

    // Pass it on
    out.write(buf, res);
    if (out.bad())
        throw wibble::exception::System("writing to destination stream");
    return true;
}

bool IODispatcher::discard_fd(int in)
{
    // Read data from child
    char buf[4096*2];
    ssize_t res = read(in, buf, 4096*2);
    if (res < 0)
        throw wibble::exception::System("reading from child stderr");
    if (res == 0)
        return false;
    return true;
}

size_t IODispatcher::send(const void* buf, size_t size)
{
    struct timespec* timeout_param = NULL;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    size_t written = 0;
    while (written < size && infd != -1)
    {
        int nfds = infd;

        fd_set infds;
        FD_ZERO(&infds);
        FD_SET(infd, &infds);

        fd_set outfds;
        FD_ZERO(&outfds);

        fd_set* poutfds;
        if (outfd != -1 || errfd != -1)
        {
            if (outfd != -1)
            {
                FD_SET(outfd, &outfds);
                nfds = max(nfds, outfd);
            }
            if (errfd != -1)
            {
                FD_SET(errfd, &outfds);
                nfds = max(nfds, errfd);
            }
            poutfds = &outfds;
        } else
            poutfds = NULL;

        ++nfds;

        int res = pselect(nfds, poutfds, &infds, NULL, timeout_param, NULL);
        if (res < 0)
            throw wibble::exception::System("waiting for activity on filter child process");
        if (res == 0)
            throw wibble::exception::Consistency("timeout waiting for activity on filter child process");

        if (infd != -1 && FD_ISSET(infd, &infds))
        {
            // Ignore SIGPIPE so we get EPIPE
            Sigignore ignpipe(SIGPIPE);
            ssize_t res = write(infd, (unsigned char*)buf + written, size - written);
            if (res < 0)
            {
                if (errno == EPIPE)
                {
                    close_infd();
                }
                else
                    throw wibble::exception::System("writing to child process");
            } else {
                written += res;
            }
        }

        if (outfd != -1 && FD_ISSET(outfd, &outfds))
            read_stdout();

        if (errfd != -1 && FD_ISSET(errfd, &outfds))
            read_stderr();
    }
    return written;
}

void IODispatcher::flush()
{
    if (infd != -1)
        close_infd();

    struct timespec* timeout_param = NULL;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    while (true)
    {
        fd_set outfds;
        FD_ZERO(&outfds);
        int nfds = 0;

        if (outfd != -1)
        {
            FD_SET(outfd, &outfds);
            nfds = max(nfds, outfd);
        }
        if (errfd != -1)
        {
            FD_SET(errfd, &outfds);
            nfds = max(nfds, errfd);
        }

        if (nfds == 0) break;

        ++nfds;

        int res = pselect(nfds, &outfds, NULL, NULL, timeout_param, NULL);
        if (res < 0)
            throw wibble::exception::System("waiting for activity on filter child process");
        if (res == 0)
            throw wibble::exception::Consistency("timeout waiting for activity on filter child process");

        if (outfd != -1 && FD_ISSET(outfd, &outfds))
            read_stdout();

        if (errfd != -1 && FD_ISSET(errfd, &outfds))
            read_stderr();
    }
}

}
}
// vim:set ts=4 sw=4:
