#ifndef ARKI_UTILS_PROCESS_H
#define ARKI_UTILS_PROCESS_H

/*
 * postprocess - postprocessing of result data
 *
 * Copyright (C) 2008--2010  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <arki/wibble/sys/childprocess.h>

namespace arki {
namespace utils {

/**
 * ChildProcess that runs a command given as an argument vector.
 *
 * The first element of the vector is the pathname to the executable, that will
 * be used as argv[0].
 */
struct Subcommand : public wibble::sys::ChildProcess
{
    std::vector<std::string> args;

    Subcommand();
    Subcommand(const std::vector<std::string>& args);
    ~Subcommand();

    virtual int main();
};

/**
 * Dispatch input and output to and from a child process.
 *
 * This is an abstract base class to use to implement simple select loops. The
 * loop manages only one process, but allows to deal with stdin, stdout and
 * stderr at the same time, without deadlocks.
 */
struct IODispatcher
{
    /// Subprocess that does the filtering
    wibble::sys::ChildProcess& subproc;
    /// Wait timeout: use { 0, 0 } to wait forever (default: { 0, 0 })
    struct timespec conf_timeout;
    /// Pipe used to send data to the subprocess
    int infd;
    /// Pipe used to get data back from the subprocess
    int outfd;
    /// Pipe used to capture the stderr of the subprocess
    int errfd;

    IODispatcher(wibble::sys::ChildProcess& subproc);
    virtual ~IODispatcher();

    /// Start the child process, setting up pipes as needed
    void start(bool do_stdin=true, bool do_stdout=true, bool do_stderr=true);

    /// Close pipe to child process, to signal we're done sending data
    void close_infd();

    /// Close stdout pipe from child process
    void close_outfd();

    /// Close stderr pipe from child process
    void close_errfd();

    /// Called when there is data to read on stdout
    virtual void read_stdout() = 0;

    /// Called when there is data to read on stderr
    virtual void read_stderr() = 0;

    /**
     * Send data to the child process
     *
     * This runs a select() loop until all data has been sent. The select loop
     * will call read_stdout() and read_stderr() if data becomes available on
     * the child stdout or stderr.
     *
     * @returns the amount of data that has been sent. This is the same as
     * \a size if all went well, and can be less only if the child process
     * died or closed stdin before all data has been sent.
     */
    size_t send(const void* buf, size_t size);

    /// Shortcut to send a string
    size_t send(const std::string& data)
    {
        return send(data.data(), data.size());
    }

    /// Shortcut to send a buffer
    size_t send(const std::vector<uint8_t>& data)
    {
        return send(data.data(), data.size());
    }

    /**
     * Keep running a select() loop on the child stdout and stderr for as long
     * as they stay open.
     *
     * Please use close_outfd() and close_errfd() to close them when reading
     * from them gives EOF.
     */
    void flush();

    /**
     * Copy data from an input file descriptor to a stream.
     *
     * It performs only one read(), to make sure that this function does not
     * block if it is called after select detected incoming data.
     *
     * @returns false on end of file (EOF), else true
     */
    static bool fd_to_stream(int in, std::ostream& out);

    /**
     * Discard data from an input file descriptor
     *
     * It performs only one read(), to make sure that this function does not
     * block if it is called after select detected incoming data.
     *
     * @returns false on end of file (EOF), else true
     */
    static bool discard_fd(int in);
};

}
}

// vim:set ts=4 sw=4:
#endif
