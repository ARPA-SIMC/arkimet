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
#include <arki/metadata.h>

namespace arki {
class ConfigFile;

namespace postproc {
class Subcommand;
}

class Postprocess : public MetadataConsumer
{
protected:
	postproc::Subcommand* m_child;
	std::string m_command;
	int m_infd;
	int m_outfd;

	void init(const ConfigFile* cfg = 0);

public:
	/**
	 * Create a postprocessor running the command \a command, and sending the
	 * output to the file descriptor \a outfd.
	 *
	 * The configuration \a cfg is used to validate if command is allowed or
	 * not.
	 */
	Postprocess(const std::string& command, int outfd);
	Postprocess(const std::string& command, int outfd, const ConfigFile& cfg);
	// When using an ostream, it tries to get a file descriptor out of it with
	// any known method (so far it's only by checking if the underlying
	// streambuf is a wibble::stream::PosixBuf).  Otherwise, it throws
	// exception::Consistency
	Postprocess(const std::string& command, std::ostream& out);
	Postprocess(const std::string& command, std::ostream& out, const ConfigFile& cfg);
	virtual ~Postprocess();

	// Process one metadata
	virtual bool operator()(Metadata&);

	// End of processing: flush all pending data
	void flush();
};


}

// vim:set ts=4 sw=4:
#endif
