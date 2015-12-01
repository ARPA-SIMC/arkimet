#ifndef ARKI_POSTPROCESS_H
#define ARKI_POSTPROCESS_H

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
#include <arki/metadata/consumer.h>

namespace arki {

namespace postproc {
class Child;
}

class Postprocess : public metadata::Eater
{
protected:
    /// Subprocess that filters our data
    postproc::Child* m_child;
    /// Command line run in the subprocess
    std::string m_command;
    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream m_errors;

public:
    /**
     * Create a postprocessor running the command \a command, and sending the
     * output to the file descriptor \a outfd.
     *
     * The configuration \a cfg is used to validate if command is allowed or
     * not.
     */
    Postprocess(const std::string& command);
    /*
    Postprocess(const std::string& command, int outfd, const std::map<std::string, std::string>& cfg);
    Postprocess(const std::string& command, std::ostream& out);
    Postprocess(const std::string& command, std::ostream& out, const std::map<std::string, std::string>& cfg);
    */
    virtual ~Postprocess();

    /**
     * Validate this postprocessor against the given configuration
     */
    void validate(const std::map<std::string, std::string>& cfg);

    /// Fork the child process setting up the various pipes
    void start();

    /// Set the output file descriptor where we send data coming from the postprocessor
    void set_output(int outfd);

    /// Set the output stream where we send the postprocessor stderr
    void set_error(std::ostream& err);

    /**
     * Set hook to be called when the child process has produced its first
     * data, just before the data is sent to the next consumer
     */
    void set_data_start_hook(metadata::Hook* hook);

    // Process one metadata
    bool eat(std::unique_ptr<Metadata>&& md) override;

    // End of processing: flush all pending data
    void flush();
};


}

// vim:set ts=4 sw=4:
#endif
