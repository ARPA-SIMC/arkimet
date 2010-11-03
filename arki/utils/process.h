#ifndef ARKI_UTILS_PROCESS_H
#define ARKI_UTILS_PROCESS_H

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

#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <wibble/sys/childprocess.h>

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
 * Manage a child process as a filter
 */
struct FilterHandler
{
    /// Subprocess that does the filtering
    wibble::sys::ChildProcess& subproc;
    /// Wait timeout: use { 0, 0 } to wait forever (default: { 0, 0 })
    struct timespec conf_timeout;
    /// Stream to which we send stderr (default: NULL to ignore stderr)
    std::ostream* conf_errstream;
    /// Pipe used to send data to the subprocess
    int infd;
    /// Pipe used to get data back from the subprocess
    int outfd;
    /// Pipe used to capture the stderr of the subprocess
    int errfd;

    FilterHandler(wibble::sys::ChildProcess& subproc);
    ~FilterHandler();

    /// Start the child process, setting up pipes as needed
    void start();

    /// Close pipe to child process, to signal we're done sending data
    void done_with_input();

    /// Read stderr and pass it on to the error stream
    void read_stderr();

    /**
     * Wait until the child has data to be read, feeding it data in the
     * meantime.
     *
     * \a buf and \a size will be updated to point to the unwritten bytes.
     * After the function returns, \a size will contain the amount of data
     * still to be written (or 0).
     *
     * @return true if there is data to be read, false if there is no data but
     * all the buffer has been written. If it returns false, then size is
     * always 0.
     */
    bool wait_for_child_data_while_sending(const void*& buf, size_t& size);

    /// Wait until the child has data to read
    void wait_for_child_data();
};

}
}

// vim:set ts=4 sw=4:
#endif
