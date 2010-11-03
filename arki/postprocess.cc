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

#include "postprocess.h"
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/utils/process.h>
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

Postprocess::Postprocess(const std::string& command)
    : m_child(0), m_handler(0), m_command(command), m_nextfd(-1), m_out(NULL), m_err(&m_errors), data_start_hook(NULL)
{
    Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
    // Parse command into its components
    for (Splitter::const_iterator j = sp.begin(m_command); j != sp.end(); ++j)
        m_args.push_back(*j);
    //cerr << "Split \"" << m_command << "\" into: " << str::join(args.begin(), args.end(), ", ") << endl;
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

void Postprocess::set_output(int outfd)
{
    m_nextfd = outfd;
}

void Postprocess::set_output(std::ostream& out)
{
    m_out = &out;
    if (stream::PosixBuf* ps = dynamic_cast<stream::PosixBuf*>(out.rdbuf()))
        m_nextfd = ps->fd();
}

void Postprocess::set_error(std::ostream& err)
{
    m_err = &err;
}

void Postprocess::validate(const map<string, string>& cfg)
{
    using namespace wibble::operators;
    // Build the set of allowed postprocessors
    set<string> allowed;
    map<string, string>::const_iterator i = cfg.find("postprocess");
    if (i != cfg.end())
    {
        Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
        string pp = i->second;
        for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Validate the command
    if (m_args.empty())
        throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command is empty");
    if (!cfg.empty() && allowed.find(m_args[0]) == allowed.end())
    {
        throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command " + m_command + " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(allowed.begin(), allowed.end()) + ")");
    }

}

void Postprocess::start()
{
    // Expand args[0] to the full pathname and check that the program exists

    // Build a list of argv0 candidates
    vector<string> cand_argv0;
    // TODO: colon-separated $PATH-like semantics
    char* env_ppdir = getenv("ARKI_POSTPROC");
    if (env_ppdir)
        cand_argv0.push_back(str::joinpath(env_ppdir, m_args[0]));
    cand_argv0.push_back(str::joinpath(POSTPROC_DIR, m_args[0]));

    // Get the first good one from the list
    string argv0;
    for (vector<string>::const_iterator i = cand_argv0.begin();
            i != cand_argv0.end(); ++i)
        if (sys::fs::access(*i, X_OK))
            argv0 = *i;

    if (argv0.empty())
        throw wibble::exception::Consistency("running postprocessing filter", "postprocess command \"" + m_command + "\" does not exists or it is not executable; tried: " + str::join(cand_argv0.begin(), cand_argv0.end()));
    m_args[0] = argv0;

    // Spawn the command
    m_child = new utils::Subcommand(m_args);
    m_handler = new utils::FilterHandler(*m_child);
    m_handler->conf_errstream = m_err;
    m_handler->start();
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
        {
            string msg = "postprocess command \"" + m_command + "\" " + sys::process::formatStatus(res);
            if (!m_errors.str().empty())
                msg += "; stderr: " + str::trim(m_errors.str());
            throw wibble::exception::Consistency("running postprocessing filter", msg);
        }
    }
}

}
// vim:set ts=4 sw=4:
