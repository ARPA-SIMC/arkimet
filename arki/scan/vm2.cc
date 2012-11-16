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
#include <sstream>
#include <unistd.h>

#include <arki/types/area.h>
#include <arki/types/time.h>
#include <arki/types/reftime.h>
#include <arki/types/product.h>
#include <arki/types/value.h>

#include <arki/utils/vm2.h>
#include <meteo-vm2/parser.h>

using namespace std;
using namespace wibble;

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

		wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s)) 
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		std::string s((const char *)buf, size);

		if (size == 0)
			throw wibble::exception::Consistency("Empty VM2 file");
        wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}
};

static VM2Validator vm_validator;

const Validator& validator() { return vm_validator; }

}

Vm2::Vm2() : in(0), parser(0) {}

Vm2::~Vm2()
{
    close();
}

void Vm2::open(const std::string& filename)
{
	// Close the previous file if needed
	close();
	this->filename = sys::fs::abspath(filename);
	this->basename = str::basename(filename);
    this->in = new std::ifstream(filename.c_str());
	if (!in->good())
		throw wibble::exception::File(filename, "opening file for reading");
    parser = new meteo::vm2::Parser(*in);
}

void Vm2::close()
{
	filename.clear();
	if (in)
		in->close();
    delete in;
    if (parser) delete parser;
    in = 0;
    parser = 0;
}

bool Vm2::next(Metadata& md)
{
    meteo::vm2::Value value;
    std::string line;

    off_t offset = in->tellg();
    if (!parser->next(value, line))
        return false;

    size_t size = line.size();

    md.create();
    md.source = types::source::Blob::create("vm2", filename, offset, size);
    md.add_note(types::Note::create("Scanned from " + basename));

    md.set(types::reftime::Position::create(types::Time::create(value.year, value.month, value.mday, value.hour, value.min, value.sec)));
    md.set(types::area::VM2::create(value.station_id));
    md.set(types::product::VM2::create(value.variable_id));

    // Look for the comma before the value starts
    size_t pos = 13;
    pos = line.find(',', pos);
    pos = line.find(',', pos + 1);
    // Store the rest as a value
    md.set(types::Value::create(line.substr(pos+1)));

    return true;
}

}
}
// vim:set ts=4 sw=4:
