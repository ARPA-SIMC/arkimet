#ifndef ARKI_UTILS_DATAREADER_H
#define ARKI_UTILS_DATAREADER_H

/*
 * utils/datareader - Read data from files
 *
 * Copyright (C) 2010--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/libconfig.h>
#include <stddef.h>
#include <string>
#include <sys/types.h>

namespace arki {
namespace utils {

namespace datareader {
struct Reader
{
public:
	virtual ~Reader() {}

	virtual void read(off_t ofs, size_t size, void* buf) = 0;
	virtual bool is(const std::string& fname) = 0;
};
}

/**
 * Read data from files, keeping the last file open.
 *
 * This optimizes for sequentially reading data from files.
 */
class DataReader
{
protected:
	datareader::Reader* last;
	
public:
	DataReader();
	~DataReader();

	void read(const std::string& fname, off_t ofs, size_t size, void* buf);
	void flush();
};

}
}

// vim:set ts=4 sw=4:
#endif
