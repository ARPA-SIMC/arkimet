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
#include <arki/utils.h>
#include <arki/nag.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
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
		if (gzseek(fd, ofs, SEEK_SET) != ofs)
			throw wibble::exception::Consistency("seeking in " + realfname, "seek failed");

		int res = gzread(fd, buf, size);
		if (res == -1 || (size_t)res != size)
			throw wibble::exception::Consistency("reading from " + realfname, "read failed");

		last_ofs = ofs + size;
	}
};

struct IdxZlibFileReader : public Reader
{
public:
	std::string fname;
	std::string realfname;
	size_t* ofs_unc;
	size_t* ofs_comp;
	size_t idxcount;
	int fd;
	gzFile gzfd;
	off_t last_ofs;

	IdxZlibFileReader(const std::string& fname)
		: fname(fname), realfname(fname + ".gz"), ofs_unc(NULL), ofs_comp(NULL), fd(-1), gzfd(NULL), last_ofs(0)
	{
		// Read index
		readIndex(realfname + ".idx");

		fd = open(realfname.c_str(), O_RDONLY);
		if (fd < 0)
			throw wibble::exception::File(realfname, "opening file");
	}

	~IdxZlibFileReader()
	{
		if (ofs_unc != NULL) delete[] ofs_unc;
		if (ofs_comp != NULL) delete[] ofs_comp;
		if (gzfd != NULL) gzclose(gzfd);
		if (fd != -1) close(fd);
	}

	void readIndex(const std::string& idxfname)
	{
		int idxfd = open(idxfname.c_str(), O_RDONLY);
		if (idxfd == -1)
			throw wibble::exception::File(idxfname, "opening file");
		utils::HandleWatch hwidx(idxfname, idxfd);
		struct stat st;
		if (fstat(idxfd, &st) == -1)
			throw wibble::exception::File(idxfname, "checking file length");
		idxcount = st.st_size / sizeof(uint32_t) / 2;
		uint32_t* diskidx = new uint32_t[idxcount * 2];
		ssize_t res = ::read(idxfd, diskidx, idxcount * sizeof(uint32_t) * 2);
		if (res < 0 || (size_t)res != idxcount * sizeof(uint32_t) * 2)
		{
			delete[] diskidx;
			throw wibble::exception::File(idxfname, "reading index file");
		}
		hwidx.close();
		ofs_unc = new size_t[idxcount];
		ofs_comp = new size_t[idxcount];
		for (size_t i = 0; i < idxcount; ++i)
		{
			if (i == 0)
			{
				ofs_unc[i] = 0;
				ofs_comp[i] = 0;
			}
			else
			{
				ofs_unc[i] = ofs_unc[i-1] + ntohl(diskidx[(i-1) * 2]);
				ofs_comp[i] = ofs_comp[i-1] + ntohl(diskidx[(i-1) * 2 + 1]);
			}
		}
		delete[] diskidx;
	}

	bool is(const std::string& fname)
	{
		return this->fname == fname;
	}

	void reposition(off_t ofs)
	{
		size_t* lb = lower_bound(ofs_unc, ofs_unc+idxcount, (size_t)ofs);
		size_t idx = lb - ofs_unc;
		if (idx > 0) idx -= 1;
		if (gzfd != NULL)
		{
			gzclose(gzfd);
			gzfd = NULL;
		}
		off_t res = lseek(fd, ofs_comp[idx], SEEK_SET);
		if (res == -1 || (size_t)res != ofs_comp[idx])
			throw wibble::exception::File(realfname, "seeking to byte " + str::fmt(ofs_comp[idx]));

		// (Re)open the compressed file
		int fd1 = dup(fd);
		gzfd = gzdopen(fd1, "rb");
		if (gzfd == NULL)
			throw wibble::exception::File(realfname, "opening file");

		// Seek inside the compressed chunk
		int gzres = gzseek(gzfd, ofs - ofs_unc[idx], SEEK_SET);
		if (gzres < 0 || (size_t)gzres != ofs - ofs_unc[idx])
			throw wibble::exception::Consistency("seeking in " + realfname, "seek failed");
	}

	void read(off_t ofs, size_t size, void* buf)
	{
		if (gzfd == NULL || ofs != last_ofs)
		{
			// We need to seek
			reposition(ofs);
		}

		int res = gzread(gzfd, buf, size);
		if (res == -1 || (size_t)res != size)
			throw wibble::exception::Consistency("reading from " + realfname, "read failed");

		last_ofs = ofs + size;
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
		if (sys::fs::access(fname, F_OK))
			last = new datareader::FileReader(fname);
		else if (sys::fs::access(fname + ".gz.idx", F_OK))
			last = new datareader::IdxZlibFileReader(fname);
		else if (sys::fs::access(fname + ".gz", F_OK))
			last = new datareader::ZlibFileReader(fname);
	}

	// Read the data
	last->read(ofs, size, buf);
}

}
}
// vim:set ts=4 sw=4:
