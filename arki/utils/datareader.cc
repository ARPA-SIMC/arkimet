/*
 * utils/datareader - Read data from files
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils.h>
#include <arki/utils/accounting.h>
#include <arki/utils/compress.h>
#include <arki/utils/files.h>
#include <arki/nag.h>
#include <arki/iotrace.h>
#include <wibble/exception.h>
#include <wibble/string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>

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
		fd = ::open(fname.c_str(), O_RDONLY
#ifdef linux
				| O_CLOEXEC
#endif
		);
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

		acct::plain_data_read_count.incr();
        iotrace::trace_file(fname, ofs, size, "read data");
	}
};

struct ZlibFileReader : public Reader
{
public:
	std::string fname;
	std::string realfname;
	gzFile fd;
	off_t last_ofs;

	ZlibFileReader(const std::string& fname)
		: fname(fname), realfname(fname + ".gz"), fd(NULL), last_ofs(0)
	{
		// Open the new file
		fd = gzopen(realfname.c_str(), "rb");
		if (fd == NULL)
			throw wibble::exception::File(realfname, "opening file");
	}

	~ZlibFileReader()
	{
		if (fd != NULL) gzclose(fd);
	}

	bool is(const std::string& fname)
	{
		return this->fname == fname;
	}

	void read(off_t ofs, size_t size, void* buf)
	{
		if (ofs != last_ofs)
		{
			if (gzseek(fd, ofs, SEEK_SET) != ofs)
				throw wibble::exception::Consistency("seeking in " + realfname, "seek failed");

			if (ofs >= last_ofs)
				acct::gzip_forward_seek_bytes.incr(ofs - last_ofs);
			else
				acct::gzip_forward_seek_bytes.incr(ofs);
		}

		int res = gzread(fd, buf, size);
		if (res == -1 || (size_t)res != size)
			throw wibble::exception::Consistency("reading from " + realfname, "read failed");

		last_ofs = ofs + size;

		acct::gzip_data_read_count.incr();
        iotrace::trace_file(fname, ofs, size, "read data");
	}
};

struct IdxZlibFileReader : public Reader
{
public:
	std::string fname;
	std::string realfname;
	compress::SeekIndex idx;
	size_t last_block;
	int fd;
	gzFile gzfd;
	off_t last_ofs;

	IdxZlibFileReader(const std::string& fname)
		: fname(fname), realfname(fname + ".gz"), last_block(0), fd(-1), gzfd(NULL), last_ofs(0)
	{
		// Read index
		idx.read(realfname + ".idx");

		fd = open(realfname.c_str(), O_RDONLY);
		if (fd < 0)
			throw wibble::exception::File(realfname, "opening file");
	}

	~IdxZlibFileReader()
	{
		if (gzfd != NULL) gzclose(gzfd);
		if (fd != -1) close(fd);
	}

	bool is(const std::string& fname)
	{
		return this->fname == fname;
	}

	void reposition(off_t ofs)
	{
		size_t block = idx.lookup(ofs);
		if (block != last_block || gzfd == NULL)
		{
			if (gzfd != NULL)
			{
				gzclose(gzfd);
				gzfd = NULL;
			}
			off_t res = lseek(fd, idx.ofs_comp[block], SEEK_SET);
			if (res == -1 || (size_t)res != idx.ofs_comp[block])
				throw wibble::exception::File(realfname, "seeking to byte " + str::fmt(idx.ofs_comp[block]));

			// (Re)open the compressed file
			int fd1 = dup(fd);
			gzfd = gzdopen(fd1, "rb");
			if (gzfd == NULL)
				throw wibble::exception::File(realfname, "opening file");

			last_block = block;

			acct::gzip_idx_reposition_count.incr();
		}

		// Seek inside the compressed chunk
		int gzres = gzseek(gzfd, ofs - idx.ofs_unc[block], SEEK_SET);
		if (gzres < 0 || (size_t)gzres != ofs - idx.ofs_unc[block])
			throw wibble::exception::Consistency("seeking in " + realfname, "seek failed");

		acct::gzip_forward_seek_bytes.incr(ofs - idx.ofs_unc[block]);
	}

	void read(off_t ofs, size_t size, void* buf)
	{
		if (gzfd == NULL || ofs != last_ofs)
		{
			if (gzfd != NULL && ofs > last_ofs && ofs < last_ofs + 4096)
			{
				// Just skip forward
				int gzres = gzseek(gzfd, ofs - last_ofs, SEEK_CUR);
				if (gzres < 0)
					throw wibble::exception::Consistency("seeking in " + realfname, "seek failed");
				acct::gzip_forward_seek_bytes.incr(ofs - last_ofs);
			} else {
				// We need to seek
				reposition(ofs);
			}
		}

		int res = gzread(gzfd, buf, size);
		if (res == -1 || (size_t)res != size)
			throw wibble::exception::Consistency("reading from " + realfname, "read failed");

		last_ofs = ofs + size;

		acct::gzip_data_read_count.incr();
        iotrace::trace_file(fname, ofs, size, "read data");
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
		if (sys::fs::exists(fname))
			last = new datareader::FileReader(fname);
		else if (sys::fs::exists(fname + ".gz.idx"))
			last = new datareader::IdxZlibFileReader(fname);
		else if (sys::fs::exists(fname + ".gz"))
			last = new datareader::ZlibFileReader(fname);
		else
			throw wibble::exception::Consistency("accessing file " + fname, "file does not exist");
	}

	// Read the data
	last->read(ofs, size, buf);
}

}
}
// vim:set ts=4 sw=4:
