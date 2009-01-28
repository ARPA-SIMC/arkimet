/*
 * dataset/ondisk/maint/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/common.h>
#include <arki/dataset/index.h>
#include <arki/metadata.h>
#include <arki/utils.h>
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
namespace maint {

struct NormalAccess
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



Datafile::Datafile(Directory* parent, const std::string& basename)
	: parent(parent)
{
	pathname = wibble::str::joinpath(parent->fullpath(), basename);
	relname = wibble::str::joinpath(parent->relpath(), basename);
}

Datafile::~Datafile()
{
}

string Datafile::extension() const
{
	// Split the name in name and ext
    size_t pos = pathname.rfind('.');
	if (pos == string::npos)
		// We need an extension to represent the type of data within
		throw wibble::exception::Consistency("getting extension of file " + pathname, "file name has no extension");
	return pathname.substr(pos+1);
}

void Datafile::rebuildSummary(bool reindex)
{
//	if (hasRebuildFlagfile(pathname))
//		throw wibble::exception::Consistency("rebuilding summary on file " + pathname, "file has rebuild flagfile");

	// Rebuild the summary by reading the metadata
	Summary summary;
	mergeFromMetadata(summary, pathname + ".metadata");

	// Perform an atomic write
	string sumfile = pathname + ".summary";
	string tmpfile = pathname + ".summary.tmp";
	Directory::writeSummary(summary, tmpfile.c_str());
	if (::rename(tmpfile.c_str(), sumfile.c_str()) != 0)
		throw wibble::exception::System("renaming " + tmpfile + " into " + sumfile);
}

bool mdcompare(const Metadata& md1, const Metadata& md2)
{
	// Compare reference time first, because we care about it
	UItem<> rt1 = md1.get(types::TYPE_REFTIME);
	UItem<> rt2 = md2.get(types::TYPE_REFTIME);
	if (int res = rt1.compare(rt2)) return res < 0;

	// Let Metadata do the rest
	return md1 < md2;
}

struct RepackConsumer : public MetadataConsumer
{
	MetadataConsumer& deleted;
	vector<Metadata> good;

	RepackConsumer(MetadataConsumer& deleted) : deleted(deleted) {}

	virtual bool operator()(Metadata& md)
	{
		// Skip deleted metadata
		if (md.deleted)
		{
			// Put the data inline
			wibble::sys::Buffer buf = md.getData();
			md.setInlineData(md.source->format, buf);
			deleted(md);
		} else
			good.push_back(md);
		return true;
	}
};

void Datafile::repack(MetadataConsumer& mdc)
{
	// Don't touch the summary, since we don't do anything that invalidates it
	// in any way
	
	// Read and sort all the metadata
	RepackConsumer sorter(mdc);

	// Collect all non-deleted metadata into a vector
	Metadata::readFile(pathname + ".metadata", sorter);
	vector<Metadata>& allmd = sorter.good;
	sort(allmd.begin(), allmd.end(), mdcompare);

	// Write out the data
	string outfname = pathname + ".new";
    std::ofstream outfile;
	outfile.open(outfname.c_str(), ios::out | ios::trunc);
	if (!outfile.is_open() || outfile.fail())
		throw wibble::exception::File(outfname, "opening file for writing");
 
 	// Write out the metadata
	string mdfname = pathname + ".metadata";
	string outmdfname = mdfname + ".new";
    std::ofstream outmd;
	outmd.open(outmdfname.c_str(), ios::out | ios::trunc);
	if (!outmd.is_open() || outmd.fail())
		throw wibble::exception::File(outmdfname, "opening file for writing");

	string fname = str::basename(pathname);
	string ext = extension();

	size_t pos = 0;
	for (vector<Metadata>::iterator i = allmd.begin();
			i != allmd.end(); ++i)
	{
		size_t ofs = outmd.tellp();
		wibble::sys::Buffer buf = i->getData();
		outfile.write((const char*)buf.data(), buf.size());

		i->source = types::source::Blob::create(ext, fname, pos, buf.size());
		pos += buf.size();
		i->write(outmd, outmdfname);

		parent->addToIndex(*i, relname, ofs);
	}
	
	outfile.close();
	outmd.close();

	// Perform the final atomic rename

	// Create rebuild flagfile to mark that we are messing with the datafile
	createNewRebuildFlagfile(pathname);

	// Reset the index for this file
	parent->resetIndex(relname);

	// Datafile first, so if renaming the metadata fails, we still have the
	// rebuild flagfile that will cause its rebuild
	if (::rename(outfname.c_str(), pathname.c_str()) != 0)
		throw wibble::exception::System("renaming " + outfname + " into " + pathname);
	
	// Then metadata
	if (::rename(outmdfname.c_str(), mdfname.c_str()) != 0)
		throw wibble::exception::System("renaming " + outmdfname + " into " + mdfname);

	removeRebuildFlagfile(pathname);
	removePackFlagfile(pathname);
}

void Datafile::rebuild(MetadataConsumer& salvage, bool reindex)
{
	// Remove the summary file if it exists
	utils::removeFlagfile(pathname + ".summary");
	string ext = extension();

	// Reset the index for this file
	if (reindex) parent->resetIndex(relname);

	// Recreate the metadata file
	string outmdfname = pathname + ".metadata";
    std::ofstream outmd;
	outmd.open(outmdfname.c_str(), ios::out | ios::trunc);
	if (!outmd.is_open() || outmd.fail())
		throw wibble::exception::File(outmdfname, "opening file for writing");

	// Rescan the file and regenerate the metadata file
	bool processed = false;
#ifdef HAVE_GRIBAPI
	if (ext == "grib1" || ext == "grib2")
	{
		scan::Grib scanner;
		scanner.open(pathname);
		Metadata md;
		while (scanner.next(md))
		{
			size_t ofs = outmd.tellp();
			Item<types::source::Blob> blob = md.source.upcast<types::source::Blob>();
			md.source = types::source::Blob::create(blob->format, str::basename(blob->filename), ofs, blob->size);
			if (reindex && !md.deleted)
				try {
					parent->addToIndex(md, relname, ofs);
				} catch (Index::DuplicateInsert) {
					salvage(md);
					md.deleted = true;
				}
			if (md.deleted)
				createPackFlagfile(pathname);
			md.write(outmd, outmdfname);
		}
		processed = true;
	}
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr") {
		scan::Bufr scanner;
		scanner.open(pathname);
		Metadata md;
		while (scanner.next(md))
		{
			size_t ofs = outmd.tellp();
			Item<types::source::Blob> blob = md.source.upcast<types::source::Blob>();
			md.source = types::source::Blob::create(blob->format, str::basename(blob->filename), ofs, blob->size);
			if (reindex && !md.deleted)
				try {
					parent->addToIndex(md, relname, ofs);
				} catch (Index::DuplicateInsert) {
					salvage(md);
					md.deleted = true;
				}
			if (md.deleted)
				createPackFlagfile(pathname);
			md.write(outmd, outmdfname);
		}
		processed = true;
	}
#endif
	if (!processed)
		throw wibble::exception::Consistency("rebuilding " + pathname, "rescanning \"" + ext + "\" is not yet implemented");

	outmd.close();

	// Rebuild our summary
	rebuildSummary(false);

	// All done so far: metadata and summary are in a consistent state
	removeRebuildFlagfile(pathname);
}

void Datafile::reindex(MetadataConsumer& salvage)
{
	// Ensure that there are no rebuild or repack flagfiles
	if (hasRebuildFlagfile(pathname))
		throw wibble::exception::Consistency("reindexing " + pathname, "rebuild flagfile exists: perform a rebuild first");

	string fname = pathname + ".metadata";

	// Read all the metadata
    std::ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	// Reindex all the metadata
	Metadata md;
	while (true)
	{
		size_t offset = in.tellg();
		if (!md.read(in, fname))
			break;
		// Skip deleted metadata
		if (md.deleted) continue;
		try {
			parent->addToIndex(md, relname, offset);
		} catch (Index::DuplicateInsert) {
			// The datum has been found to be duplicated: mark it as deleted
			salvage(md);
			auto_ptr<NormalAccess> na(new NormalAccess(pathname));
			na->remove(offset);
		}
	}

	in.close();
}

}
}
}
}
// vim:set ts=4 sw=4:
