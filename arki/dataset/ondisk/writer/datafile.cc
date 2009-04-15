/*
 * dataset/ondisk/writer/datafile - Handle a data file plus its associated files
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

#include <config.h>
#include <arki/dataset/ondisk/writer/datafile.h>
#include <arki/dataset/ondisk/writer/directory.h>
#include <arki/dataset/index.h>
#include <arki/metadata.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;
using namespace arki::utils::files;

static void mergeFromMetadata(arki::Summary& merger, const std::string& fname)
{
	arki::Metadata md;
	ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");
	while (md.read(in, fname))
		if (!md.deleted)
			merger.add(md);
}


namespace arki {
namespace dataset {
namespace ondisk {
namespace writer {

struct Datafile::NormalAccess
{
	const std::string& pathname;
	int datafd, mdfd;
	off_t dataofs, mdofs;
	bool haveErrors;

	void errorHappened()
	{
		if (!haveErrors)
		{
			haveErrors = true;
			// When update errors happen, we remove the summary file
			utils::removeFlagfile(pathname + ".summary");
		}
	}

	NormalAccess(const std::string& pathname)
		: pathname(pathname), datafd(-1), mdfd(-1), haveErrors(false)
	{
		createNewRebuildFlagfile(pathname);

		// Note: we could use O_WRONLY instead of O_APPEND because we keep a
		// local copy of the append offset, and in case of concurrent appends
		// we would fail anyway.  However, we use O_APPEND to make sure we
		// don't accidentally overwrite things, ever

		try {
			string fname = pathname;

			// Open the data file
			datafd = ::open(fname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
			if (datafd == -1)
				throw wibble::exception::File(fname, "opening file for appending data");

			// Get the write position in the data file
			dataofs = lseek(datafd, 0, SEEK_END);
			if (dataofs == (off_t)-1)
				throw wibble::exception::File(fname, "reading the current position");

			string mdfname = pathname + ".metadata";

			// Open the metadata file
			mdfd = ::open(mdfname.c_str(), O_WRONLY | O_CREAT, 0666);
			if (mdfd == -1)
				throw wibble::exception::File(mdfname, "opening file for appending data");

			// Get the write position in the metadata file
			mdofs = lseek(mdfd, 0, SEEK_END);
			if (mdofs == (off_t)-1)
				throw wibble::exception::File(mdfname, "reading the current position");
		} catch (...) {
			errorHappened();
			throw;
		}
	}
	~NormalAccess()
	{
		try {
			// Close files
			if (datafd != -1 && ::close(datafd) == -1)
				throw wibble::exception::File(pathname, "Closing file");
			if (mdfd != -1 && ::close(mdfd) == -1)
				throw wibble::exception::File(pathname + ".metadata", "Closing file");
		} catch (...) {
			errorHappened();
			throw;
		}

		if (!haveErrors)
			removeRebuildFlagfile(pathname);
	}

	#if 0
	void replace(Metadata& md, size_t oldofs);
	#endif
	void append(Metadata& md)
	{
		try {
			UItem<types::Source> origSource = md.source;

			// Read initial GRIB source data
			wibble::sys::Buffer buf = md.getData();

			// Set the source information that we are writing in the metadata
			md.source = types::source::Blob::create(origSource->format, str::basename(pathname), dataofs, buf.size());
			
			// Encode the metadata
			string encoded = md.encode();

			// Append the data
			ssize_t res = write(datafd, buf.data(), buf.size());
			if (res < 0 || (unsigned)res != buf.size())
				throw wibble::exception::File(pathname, "writing " + str::fmt(buf.size()) + " bytes to the file");

			size_t ofs = dataofs;

			// Update the append offset
			dataofs += buf.size();

			// Append the metadata
			res = write(mdfd, encoded.data(), encoded.size());
			if (res < 0 || (unsigned) res != encoded.size())
				throw wibble::exception::File(pathname + ".metadata", "writing " + str::fmt(encoded.size()) + " bytes to the file");

			// Update the append offset
			mdofs += encoded.size();
			
			// Put the full path in the Source of the metadata that we pass on
			md.source = types::source::Blob::create(origSource->format, pathname, ofs, buf.size());
		} catch (...) {
			errorHappened();
			throw;
		}
	}

	#if 0
	void NormalAccess::replace(Metadata& md, size_t oldofs)
	{
	}
	#endif

	void remove(size_t oldofs)
	{
		try {
			// Mark the old metadata as deleted
			if (pwrite(mdfd, "!", 1, oldofs) != 1)
				throw wibble::exception::File(pathname + ".metadata", "marking old metadata as deleted");
			createPackFlagfile(pathname);
		} catch (...) {
			errorHappened();
			throw;
		}
	}
};

/**
 * SummaryAccess can be in two states:
 * \l valid summary in memory, gets appended to as data is added to
 *    the datafile and written on exit if it has diverged from the ondisk
 *    version.
 * \l nothing in memory, ondisk deleted, needs rebuild on exit.  Used when
 *    removing data from the datafile, or when repacking.
 */
struct Datafile::SummaryAccess
{
	const std::string& pathname;

	Summary summary;
	bool dirty, invalid;

	SummaryAccess(const std::string& pathname, bool invalid = false)
		: pathname(pathname), dirty(false), invalid(invalid)
	{
		if (invalid)
			utils::removeFlagfile(pathname + ".summary");
		else {
			string name = pathname + ".summary";
			if (utils::hasFlagfile(name))
				// Read it from the on-disk summary
				Directory::readSummary(summary, name);
			else {
				string name = pathname + ".metadata";
				if (utils::hasFlagfile(name))
				{
					// Fill it up rereading all the metadata
					mergeFromMetadata(summary, pathname + ".metadata");
					dirty = true;
				}
			}
		}
	}
	~SummaryAccess()
	{
		flush();
	}

	void flush()
	{
		if (invalid)
		{
			// Rebuild by rereading all the metadata
			summary.clear();
			mergeFromMetadata(summary, pathname + ".metadata");
			dirty = true;
			invalid = false;
		}
		if (dirty) {
			Directory::writeSummary(summary, pathname + ".summary");
			dirty = false;
		}
	}

	/// Add the given metadata to the summary
	void addToSummary(Metadata& md)
	{
		// If the summary has been invalidated, do nothing: we have to rebuild
		// it anyway
		if (invalid) return;

		// Add the metadata to the summary
		summary.add(md);
		dirty = true;
	}

	/**
	 * Invalidate the summary, so that that it will be rebuild by merging its
	 * sources again.
	 */
	void invalidate()
	{
		invalid = true;
		summary.clear();
		utils::removeFlagfile(pathname + ".summary");
	}
};




Datafile::Datafile(Directory* parent, const std::string& name, const std::string& ext)
	: parent(parent), name(name), ext(ext), na(0), sa(0), summaryIsInvalid(false)
{
	pathname = wibble::str::joinpath(parent->fullpath(), name + '.' + ext);
	relname = wibble::str::joinpath(parent->relpath(), name + '.' + ext);
}

Datafile::~Datafile()
{
	flushCached();
}

void Datafile::flushCached()
{
	if (na)
	{
		delete na;
		na = 0;
	}
	if (sa)
	{
		delete sa;
		sa = 0;
	}
}

off_t Datafile::nextOffset()
{
	if (!na) na = new NormalAccess(pathname);
	return na->mdofs;
}

// Get the summary for this datafile
const Summary& Datafile::summary()
{
	if (!sa) sa = new SummaryAccess(pathname);
	return sa->summary;
}

void Datafile::append(Metadata& md)
{
	if (!na) na = new NormalAccess(pathname);
	if (na->haveErrors)
		throw wibble::exception::Consistency("appending to " + pathname, "problems happened with this file in the past, so updates are refused until the file is rebuilt");
	if (!sa) sa = new SummaryAccess(pathname);

	//parent->addToIndex(md, str::basename(pathname), na->mdofs);
	na->append(md);

	sa->addToSummary(md);

	// Propagate to parent
	parent->addToSummary(md);
}

void Datafile::remove(size_t oldofs)
{
	if (!na) na = new NormalAccess(pathname);
	if (na->haveErrors)
		throw wibble::exception::Consistency("appending to " + pathname, "problems happened with this file in the past, so updates are refused until the file is rebuilt");

	na->remove(oldofs);

	if (sa)
		sa->invalidate();
	else {
		utils::removeFlagfile(pathname + ".summary");
		sa = new SummaryAccess(pathname);
	}

	parent->invalidateSummary();
}

}
}
}
}
// vim:set ts=4 sw=4:
