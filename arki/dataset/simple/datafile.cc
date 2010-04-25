/*
 * dataset/simple/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/simple/datafile.h>
#include <arki/metadata.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/scan/any.h>
#include <arki/nag.h>

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
namespace simple {

Datafile::Datafile(const std::string& pathname)
	: pathname(pathname), basename(str::basename(pathname)), fd(-1)
{
	if (sys::fs::access(pathname, F_OK))
	{
		// Read the metadata
		scan::scan(pathname, mds);

		// Read the summary
		if (!mds.empty())
		{
			string sumfname = pathname + ".summary";
			if (utils::files::timestamp(pathname) <= utils::files::timestamp(sumfname))
				sum.readFile(sumfname);
			else
			{
				utils::metadata::Summarise s(sum);
				mds.sendTo(s);
			}
		}
	}

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
	flush();
}

void Datafile::lock()
{
	struct flock lock;
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = 0;
	lock.l_len = 0;
	// Use SETLKW, so that if it is already locked, we just wait
	if (fcntl(fd, F_SETLKW, &lock) == -1)
		throw wibble::exception::System("locking the file " + pathname + " for writing");
}

void Datafile::unlock()
{
	struct flock lock;
	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = 0;
	lock.l_len = 0;
	fcntl(fd, F_SETLK, &lock);
}

void Datafile::flush()
{
	mds.writeAtomically(pathname + ".metadata");
	sum.writeAtomically(pathname + ".summary");
}

void Datafile::append(Metadata& md)
{
	// Make a backup of the original source metadata item
	UItem<types::Source> origSource = md.source;

	// Get the data blob to append
	sys::Buffer buf = md.getData();

	// Lock the file so that we are the only ones writing to it
	lock();

	// Get the write position in the data file
	off_t wrpos = lseek(fd, 0, SEEK_END);
	if (wrpos == (off_t)-1)
		throw wibble::exception::File(pathname, "reading the current position");

	// Set the source information that we are writing in the metadata
	md.source = types::source::Blob::create(origSource->format, basename, wrpos, buf.size());

	// Prevent caching (ignore function result)
	(void)posix_fadvise(fd, wrpos, buf.size(), POSIX_FADV_DONTNEED);

	try {
		// Append the data
		ssize_t res = write(fd, buf.data(), buf.size());
		if (res < 0 || (unsigned)res != buf.size())
			throw wibble::exception::File(pathname, "writing " + str::fmt(buf.size()) + " bytes to " + pathname);

		if (fdatasync(fd) < 0)
			throw wibble::exception::File(pathname, "flushing write to " + pathname);
	} catch (...) {
		// Damage mitigation, useful at least after a partial append in
		// case of disk full
		md.source = origSource;

		// If we had a problem, attempt to truncate the file to the original size
		if (ftruncate(fd, wrpos) == -1)
			nag::warning("truncating %s to previous size %zd abort append: %m", pathname.c_str(), wrpos);

		unlock();

		throw;
	}

	unlock();

	mds.push_back(md);
	sum.add(md);
}
			
}
}
}
// vim:set ts=4 sw=4:
