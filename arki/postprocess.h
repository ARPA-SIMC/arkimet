#ifndef ARKI_POSTPROCESS_H
#define ARKI_POSTPROCESS_H

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

#include <string>
#include <map>
#include <arki/metadata/consumer.h>

namespace arki {
namespace postproc {
class Subcommand;
class FilterHandler;
}

class Postprocess : public metadata::Consumer
{
protected:
    /// Subprocess that filters our data
    postproc::Subcommand* m_child;
    /// Filter handler that takes care of data transfer coordination
    postproc::FilterHandler* m_handler;
    /// Command line run in the subprocess
    std::string m_command;
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
    /// Non-null if we should notify the hook as soon as some data arrives from the processor
    metadata::Hook* data_start_hook;

    /// Same as init with an empty config
    void init();

    /**
     * Validate this postprocessor against the given configuration and then
     * fork the child process setting up the various pipes
     */
    void init(const std::map<std::string, std::string>& cfg);

    /**
     * Write the given buffer to the child, pumping out the child output in the meantime
     *
     * @returns true if there is still data to read from the child, false if
     * the child closed its output pipe.
     */
    bool pump(const void* buf, size_t size);

    /// Just pump all the remaining child output out
    void pump();

    /**
     * Pass on data from the child postprocessor to the destination
     *
     * @return true if there are still data to read, false on EOF from child
     * process
     */
    bool read_and_pass_on();

public:
    /**
     * Create a postprocessor running the command \a command, and sending the
     * output to the file descriptor \a outfd.
     *
     * The configuration \a cfg is used to validate if command is allowed or
     * not.
     */
    Postprocess(const std::string& command, int outfd);
    Postprocess(const std::string& command, int outfd, const std::map<std::string, std::string>& cfg);
    // When using an ostream, it tries to get a file descriptor out of it with
    // any known method (so far it's only by checking if the underlying
    // streambuf is a wibble::stream::PosixBuf).  Otherwise, it throws
    // exception::Consistency
    Postprocess(const std::string& command, std::ostream& out);
    Postprocess(const std::string& command, std::ostream& out, const std::map<std::string, std::string>& cfg);
    virtual ~Postprocess();

    void set_data_start_hook(metadata::Hook* hook);

    // Process one metadata
    virtual bool operator()(Metadata&);

    // End of processing: flush all pending data
    void flush();
};


}

// vim:set ts=4 sw=4:
#endif
