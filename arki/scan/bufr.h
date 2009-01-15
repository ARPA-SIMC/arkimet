#ifndef ARKI_SCAN_BUFR_H
#define ARKI_SCAN_BUFR_H

/*
 * scan/bufr - Scan a BUFR file for metadata.
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <dballe/core/rawmsg.h>
#include <dballe/core/file.h>
#include <dballe/bufrex/msg.h>
#include <string>

namespace arki {
class Metadata;

namespace scan {

/**
 * Scan files for BUFR messages
 */
class Bufr
{
	std::string filename;
	dba_rawmsg rmsg;
	bufrex_msg msg;
	dba_file file;
	bool m_inline_data;

public:
	Bufr(bool inlineData = false);
	~Bufr();

	/**
	 * Access a file with BUFR data
	 */
	void open(const std::string& filename);

	/**
	 * Close the input file.
	 *
	 * This is optional: the file will be closed by the destructor if needed.
	 */
	void close();

	/**
	 * Scan the next BUFR in the file.
	 *
	 * @returns
	 *   true if it found a BUFR message,
	 *   false if there are no more BUFR messages in the file
	 */
	bool next(Metadata& md);
};

}
}

// vim:set ts=4 sw=4:
#endif
