/*
 * dataset/ondisk2/writer/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"
#include <arki/dataset/ondisk2/writer/datafile.h>
#include <arki/metadata.h>
#include <arki/utils.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

struct Append : public Transaction
{
	bool fired;
	Datafile& df;
	Metadata& md;
	UItem<types::Source> origSource;
	wibble::sys::Buffer buf;
	off_t pos;

	Append(Datafile& df, Metadata& md) : fired(false), df(df), md(md)
   	{
		// Make a backup of the original source metadata item
		origSource = md.source;

		// Get the data blob to append
		buf = md.getData();

		// Lock the file so that we are the only ones writing to it
		lock();

		// Insertion offset
		pos = wrpos();
	}
	virtual ~Append()
	{
		if (!fired) rollback();
	}

	void lock()
	{
		struct flock lock;
		lock.l_type = F_WRLCK;
		lock.l_whence = SEEK_SET;
		lock.l_start = 0;
		lock.l_len = 0;
		// Use SETLKW, so that if it is already locked, we just wait
		if (fcntl(df.fd, F_SETLKW, &lock) == -1)
			throw wibble::exception::System("locking the file " + df.pathname + " for writing");
	}

	void unlock()
	{
		struct flock lock;
		lock.l_type = F_UNLCK;
		lock.l_whence = SEEK_SET;
		lock.l_start = 0;
		lock.l_len = 0;
		fcntl(df.fd, F_SETLK, &lock);
	}

	off_t wrpos()
	{
		// Get the write position in the data file
		off_t size = lseek(df.fd, 0, SEEK_END);
		if (size == (off_t)-1)
			throw wibble::exception::File(df.pathname, "reading the current position");
		return size;
	}

	virtual void commit()
	{
		if (fired) return;

		// Set the source information that we are writing in the metadata
		md.source = types::source::Blob::create(origSource->format, df.pathname, pos, buf.size());

		// Append the data
		ssize_t res = write(df.fd, buf.data(), buf.size());
		if (res < 0 || (unsigned)res != buf.size())
			throw wibble::exception::File(df.pathname, "writing " + str::fmt(buf.size()) + " bytes to " + df.pathname);

		if (fdatasync(df.fd) < 0)
			throw wibble::exception::File(df.pathname, "flushing write to " + df.pathname);

		unlock();
		fired = true;
	}

	virtual void rollback()
	{
		if (fired) return;

		md.source = origSource;

		// If we had a problem, attempt to truncate the file to the original size
		if (ftruncate(df.fd, pos) == -1)
			throw wibble::exception::File(df.pathname, "truncating file to previous position (" + str::fmt(pos) + ")");

		unlock();
		fired = true;
	}
};

Datafile::Datafile(const std::string& pathname) : pathname(pathname), fd(-1)
{
	// Note: we could use O_WRONLY instead of O_APPEND because we keep a
	// local copy of the append offset, and in case of concurrent appends
	// we would fail anyway.  However, we use O_APPEND to make sure we
	// don't accidentally overwrite things, ever

	// Open the data file
	fd = ::open(pathname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
	if (fd == -1)
		throw wibble::exception::File(pathname, "opening file for appending data");
}

Datafile::~Datafile()
{
	// Close files
	if (fd != -1 && ::close(fd) == -1)
		throw wibble::exception::File(pathname, "Closing file");
}

Pending Datafile::append(Metadata& md, off_t* ofs)
{
	Append* res = new Append(*this, md);
	*ofs = res->pos;
	return res;
}
			
}
}
}
}
// vim:set ts=4 sw=4:
