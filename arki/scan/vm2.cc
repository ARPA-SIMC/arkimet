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

#include "config.h"
#include <arki/scan/vm2.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/sys/fs.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>
#include <cstring>
#include <unistd.h>

#include <arki/types/area.h>
#include <arki/types/time.h>
#include <arki/types/reftime.h>
#include <arki/types/product.h>

using namespace std;
using namespace wibble;

#define VM2_REGEXP "^[0-9]{12},[0-9]+,[0-9]+,.*,.*,.*,.*\n$"

namespace arki {
namespace scan {

namespace vm2 {

struct VM2Validator : public Validator
{
	virtual ~VM2Validator() {}

	// Validate data found in a file
	virtual void validate(int fd, off64_t offset, size_t size, const std::string& fname) const
	{
		char buf[1024];

		if (pread(fd, buf, size, offset) == -1)
			throw wibble::exception::System(fname);

        std::string s((const char *)buf, size);

		wibble::Regexp re(VM2_REGEXP, 0, REG_EXTENDED);
		if (!re.match(s)) 
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		std::string s((const char *)buf, size);

		if (size == 0)
			throw wibble::exception::Consistency("Empty VM2 file");
		wibble::Regexp re(VM2_REGEXP, 0, REG_EXTENDED);
		if (!re.match(s))
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}
};

static VM2Validator vm_validator;

const Validator& validator() { return vm_validator; }

}

Vm2::Vm2() : in(NULL), offset(0), size(0) {}

Vm2::~Vm2()
{
	if (in) fclose(in);
}

void Vm2::open(const std::string& filename)
{
	// Close the previous file if needed
	close();
	this->filename = sys::fs::abspath(filename);
	this->basename = str::basename(filename);
	if (!(in = fopen(filename.c_str(), "r")))
		throw wibble::exception::File(filename, "opening file for reading");
}

void Vm2::close()
{
	filename.clear();
	if (in)
	{
		fclose(in);
		in = 0;
		offset = 0;
		size = 0;
	}
}

bool Vm2::next(Metadata& md)
{
	char buf[1024];
	
	if (fgets(buf, sizeof buf, in) == NULL)
		return false;

	std::string line(buf);

	size = line.size();
	offset += size;

	md.create();
	setSource(md);

	using wibble::str::Split;
	Split splitter(",", line);

	std::vector<std::string> vals;
	for (Split::const_iterator i = splitter.begin();
	     i != splitter.end(); ++i)
	  vals.push_back(*i);
	
	int y, m, d, ho, mi;
	sscanf(vals.at(0).c_str(), "%04d%02d%02d%02d%02d", &y, &m, &d, &ho, &mi);
	using arki::types::reftime::Position;
	md.set(Position::decodeString(str::fmtf("%04d-%02d-%02d %02d:%02d:00", y, m, d, ho, mi)));

    unsigned long station_id = strtoul(vals.at(1).c_str(), NULL, 10);
    unsigned long variable_id = strtoul(vals.at(2).c_str(), NULL, 10);

    md.set(types::area::VM2::create(station_id));
    md.set(types::product::VM2::create(variable_id));

	return true;
}

void Vm2::setSource(Metadata& md)
{
	md.source = types::source::Blob::create("vm2", filename, offset - size, size);
	md.add_note(types::Note::create("Scanned from " + basename));
}

}
}
// vim:set ts=4 sw=4:
