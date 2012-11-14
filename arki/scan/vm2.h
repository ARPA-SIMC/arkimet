#ifndef ARKI_SCAN_VM2_H
#define ARKI_SCAN_VM2_H

/*
 * scan/vm2 - Scan a VM2 file for metadata
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>
 */

#include <string>
#include <vector>
#include <unistd.h>
#include <fstream>

namespace vm2 {
class Parser;
}

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace vm2 {
const Validator& validator();
}

class Vm2
{
protected:
    std::ifstream* in;
	std::string filename;
	std::string basename;
    unsigned lineno;

    ::vm2::Parser* parser;

public:
	Vm2();
	virtual ~Vm2();

	/**
	 * Access a file with VM2 data
	 */
	void open(const std::string& filename);

	/**
	 * Close the input file.
	 *
	 * This is optional: the file will be closed by the destructor if needed.
	 */
	void close();

	/**
	 * Scan the next VM2 in the file.
	 *
	 * @returns
	 *   true if it found a VM2 message,
	 *   false if there are no more VM2 messages in the file
	 */
	bool next(Metadata& md);

};

}
}

// vim:set ts=4 sw=4:
#endif
