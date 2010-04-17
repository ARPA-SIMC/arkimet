/*
 * utils/datareader - Read data from files
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/datareader.h>
#include <arki/nag.h>
#include <wibble/exception.h>
#include <wibble/string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {

namespace datareader {

struct FileReader : public Reader
{
public:
	std::string fname;
	int fd;

	FileReader(const std::string& fname)
		: fname(fname)
	{
		// Open the new file
		fd = ::open(fname.c_str(), O_RDONLY | O_CLOEXEC);
		if (fd == -1)
			throw wibble::exception::File(fname, "opening file");
	}

	~FileReader()
	{
		::close(fd);
	}

	bool is(const std::string& fname)
	{
		return this->fname == fname;
	}

	void read(off_t ofs, size_t size, void* buf)
	{
		if (posix_fadvise(fd, ofs, size, POSIX_FADV_DONTNEED) != 0)
			nag::debug("fadvise on %s failed: %m", fname.c_str());
		ssize_t res = pread(fd, buf, size, ofs);
		if (res < 0)
			throw wibble::exception::File(fname, "reading " + str::fmt(size) + " bytes at " + str::fmt(ofs));
		if ((size_t)res != size)
			throw wibble::exception::Consistency("reading from " + fname, "read only " + str::fmt(res) + "/" + str::fmt(size) + " bytes at " + str::fmt(ofs));
	}
};

}

DataReader::DataReader() : last(0) {}
DataReader::~DataReader()
{
	flush();
}

void DataReader::flush()
{
	if (last)
	{
		delete last;
		last = 0;
	}
}

void DataReader::read(const std::string& fname, off_t ofs, size_t size, void* buf)
{
	if (!last || !last->is(fname))
	{
		// Close the last file
		flush();

		// Open the new file
		last = new datareader::FileReader(fname);
	}

	// Read the data
	last->read(ofs, size, buf);
}

}
}
// vim:set ts=4 sw=4:
