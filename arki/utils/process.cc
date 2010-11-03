/*
 * postprocess - postprocessing of result data
 *
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "process.h"
#include <wibble/sys/childprocess.h>
#include <wibble/sys/process.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "config.h"
#include <signal.h>
#include <cerrno>
#include <iostream>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace wibble;

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


FilterHandler::FilterHandler(wibble::sys::ChildProcess& subproc)
    : subproc(subproc), conf_errstream(0), infd(-1), outfd(-1), errfd(-1)
{
    conf_timeout.tv_sec = 0;
    conf_timeout.tv_nsec = 0;
}

FilterHandler::~FilterHandler()
{
    if (infd != -1) close(infd);
    if (outfd != -1) close(outfd);
    if (errfd != -1) close(errfd);
}

void FilterHandler::start()
{
    subproc.forkAndRedirect(&infd, &outfd, &errfd);
}

void FilterHandler::done_with_input()
{
    if (infd != -1)
    {
        //cerr << "Closing pipe" << endl;
        if (close(infd) < 0)
            throw wibble::exception::System("closing output to filter child process");
        infd = -1;
    }
}

void FilterHandler::read_stderr()
{
    // Read data from child
    char buf[4096*2];
    ssize_t res = read(errfd, buf, 4096*2);
    if (res < 0)
        throw wibble::exception::System("reading from child stderr");
    if (res == 0)
    {
        close(errfd);
        errfd = -1;
        return;
    }

    // Pass it on
    if (conf_errstream != NULL)
    {
        conf_errstream->write(buf, res);
        if (conf_errstream->bad())
            throw wibble::exception::System("writing to destination error stream");
    }
}

bool FilterHandler::wait_for_child_data_while_sending(const void*& buf, size_t& size)
{
    struct timespec* timeout_param = NULL;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    while (size > 0)
    {
        fd_set infds;
        fd_set outfds;
        FD_ZERO(&infds);
        FD_SET(infd, &infds);
        FD_ZERO(&outfds);
        FD_SET(outfd, &outfds);
        int nfds = max(infd, outfd);
        if (errfd != -1)
        {
            FD_SET(errfd, &outfds);
            nfds = max(nfds, errfd);
        }
        ++nfds;

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

        if (errfd != -1 && FD_ISSET(errfd, &outfds))
            read_stderr();

        if (FD_ISSET(outfd, &outfds))
            return true;
    }
    return false;
}

void FilterHandler::wait_for_child_data()
{
    struct timespec* timeout_param = NULL;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    while (true)
    {
        fd_set outfds;
        FD_ZERO(&outfds);
        FD_SET(outfd, &outfds);
        FD_SET(errfd, &outfds);
        int nfds = outfd;
        if (errfd != -1)
            nfds = max(nfds, errfd);
        ++nfds;

        int res = pselect(nfds, &outfds, NULL, NULL, timeout_param, NULL);
        if (res < 0)
            throw wibble::exception::System("waiting for activity on filter child process");
        if (res == 0)
            throw wibble::exception::Consistency("timeout waiting for activity on filter child process");

        if (errfd != -1 && FD_ISSET(errfd, &outfds))
            read_stderr();

        if (FD_ISSET(outfd, &outfds))
            return;
    }
}

/*
 * One can ignore SIGPIPE (using, for example, the signal system call). In this
 * case, all system calls that would cause SIGPIPE to be sent will return -1
 * and set errno to EPIPE.
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
}
// vim:set ts=4 sw=4:
