/*
 * scan/nc - Scan a NetCDF file for metadata
 *
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/scan/nc.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <arki/utils/files.h>
#include <arki/nag.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/sys/fs.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>
#include <netcdfcpp.h>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <unistd.h>

#include <arki/types/area.h>
#include <arki/types/time.h>
#include <arki/types/reftime.h>
#include <arki/types/product.h>
#include <arki/types/value.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace scan {

namespace netcdf {

struct NetCDFValidator : public Validator
{
	virtual ~NetCDFValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
#if 0
		char buf[1024];

		if (pread(fd, buf, size, offset) == -1)
			throw wibble::exception::System(fname);

        std::string s((const char *)buf, size);

		wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
#endif
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
#if 0
		std::string s((const char *)buf, size);

		if (size == 0)
			throw wibble::exception::Consistency("Empty VM2 file");
        wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
#endif
	}
};

static NetCDFValidator nc_validator;

const Validator& validator() { return nc_validator; }

struct Backend
{
    NcFile nc;

    Backend(const std::string& fname)
        : nc(fname.c_str(), NcFile::ReadOnly)
    {
    }
};

}

NetCDF::NetCDF() : backend(0) {}

NetCDF::~NetCDF()
{
    close();
}

void NetCDF::open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::fs::abspath(filename), basedir, relname);
}

void NetCDF::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = sys::fs::abspath(filename);
    this->basedir = basedir;
    this->relname = relname;
    if (relname == "-")
        throw wibble::exception::File(filename, "cannot read NetCDF data from standard input");
    backend = new netcdf::Backend(filename);
}

void NetCDF::close()
{
    if (backend) delete backend;
    backend = 0;
}

bool NetCDF::next(Metadata& md)
{
    if (!backend) return false;

    NcFile& nc = backend->nc;

#if 0
    meteo::vm2::Value value;
    std::string line;

    off_t offset = 0;
    while (true)
    {
        offset = in->tellg();
        try {
            if (!parser->next(value, line))
                return false;
            else
                break;
        } catch (wibble::exception::Consistency& e) {
            nag::warning("Skipping VM2 line: %s", e.what());
        }
    }

    size_t size = line.size();

    md.create();
    md.source = types::source::Blob::create("vm2", basedir, relname, offset, size);
    md.setCachedData(wibble::sys::Buffer(line.c_str(), line.size()));
    md.add_note(types::Note::create("Scanned from " + relname));
    md.set(types::reftime::Position::create(types::Time::create(value.year, value.month, value.mday, value.hour, value.min, value.sec)));
    md.set(types::area::VM2::create(value.station_id));
    md.set(types::product::VM2::create(value.variable_id));

    // Look for the comma before the value starts
    size_t pos = 0;
    pos = line.find(',', pos);
    pos = line.find(',', pos + 1);
    pos = line.find(',', pos + 1);
    // Store the rest as a value
    md.set(types::Value::create(line.substr(pos+1)));

    return true;
#endif
    return false;
}

}
}