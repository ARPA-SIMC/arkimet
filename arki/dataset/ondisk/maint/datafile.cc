/*
 * dataset/ondisk/maint/datafile - Handle a data file plus its associated files
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
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/index.h>
#include <arki/metadata.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/summary.h>
#include <arki/scan/any.h>

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
using namespace arki::utils;
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

void Datafile::rebuildSummary()
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

struct RepackConsumer : public metadata::Consumer
{
	vector<Metadata> good;
	size_t removed;

	RepackConsumer() : removed(0) {}

	virtual bool operator()(Metadata& md)
	{
		// Skip deleted metadata
		if (!md.deleted)
			good.push_back(md);
		else
			++removed;
		return true;
	}
};

size_t Datafile::repack()
{
	// Don't touch the summary, since we don't do anything that invalidates it
	// in any way
	
	// Read and sort all the metadata
	RepackConsumer sorter;

	// Collect all non-deleted metadata into a vector
	Metadata::readFile(pathname + ".metadata", sorter);
	vector<Metadata>& allmd = sorter.good;
	std::sort(allmd.begin(), allmd.end(), mdcompare);

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

	// Create rebuild flagfile to mark that we are messing with the datafile
	createNewRebuildFlagfile(pathname);

	// Reset the index for this file
	parent->resetIndex(relname);

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

	// We commit the index, which is out of sync until we rename. But since
	// we have the rebuild flagfile on, we know we need to rescan the files
	// for them to make sense
	parent->commit();

	// Perform the final atomic rename

	// Datafile first, so if renaming the metadata fails, we still have the
	// rebuild flagfile that will cause its rebuild
	if (::rename(outfname.c_str(), pathname.c_str()) != 0)
		throw wibble::exception::System("renaming " + outfname + " into " + pathname);
	
	// Then metadata
	if (::rename(outmdfname.c_str(), mdfname.c_str()) != 0)
		throw wibble::exception::System("renaming " + outmdfname + " into " + mdfname);

	removeRebuildFlagfile(pathname);
	removePackFlagfile(pathname);

	return sorter.removed;
}

void Datafile::rebuild(bool reindex)
{
	// Remove the summary file if it exists
	utils::removeFlagfile(pathname + ".summary");
	string ext = extension();

	// Reset the index for this file
	if (reindex) parent->resetIndex(relname);

	// Delete the metadata file: we cannot trust it, and scan::scan would
	// use it if it finds it
	utils::removeFlagfile(pathname + ".metadata");

	// Collect the scan results in a metadata::Collector
	metadata::Collector mds;
	if (!scan::scan(pathname, mds))
		throw wibble::exception::Consistency("rebuilding " + pathname, "rescanning \"" + ext + "\" is not yet implemented");

	// Scan the list of metadata, looking for duplicates and marking all
	// the duplicates except the last one as deleted
	map<string, Metadata*> finddupes;
	for (metadata::Collector::iterator i = mds.begin(); i != mds.end(); ++i)
	{
		string id = parent->id(*i);
		if (id.empty())
			continue;
		map<string, Metadata*>::iterator dup = finddupes.find(id);
		if (dup == finddupes.end())
			finddupes.insert(make_pair(id, &(*i)));
		else
		{
			dup->second->deleted = true;
			dup->second = &(*i);
		}
	}

	// Open the new .metadata file
	string outmdfname = pathname + ".metadata";
	std::ofstream outmd;
	outmd.open(outmdfname.c_str(), ios::out | ios::trunc);
	if (!outmd.is_open() || outmd.fail())
		throw wibble::exception::File(outmdfname, "opening file for writing");

	// Save all the metadata in the file
	for (metadata::Collector::iterator i = mds.begin(); i != mds.end(); ++i)
	{
		size_t ofs = outmd.tellp();
		Item<types::source::Blob> blob = i->source.upcast<types::source::Blob>();
		blob->filename = str::basename(blob->filename);
		if (reindex && !i->deleted)
			try {
				parent->addToIndex(*i, relname, ofs);
			} catch (utils::sqlite::DuplicateInsert) {
				// If we end up here, it means that we have a
				// duplicate stored in a different file
				throw wibble::exception::Consistency("repacking " + pathname,
					"data item at offset " + str::fmt(ofs) + " has a duplicate elsewhere in the dataset: manual fix is required");
			}
		if (i->deleted)
			createPackFlagfile(pathname);
		i->write(outmd, outmdfname);
	}

	outmd.close();

	// Rebuild our summary
	rebuildSummary();

	// All done so far: metadata and summary are in a consistent state
	removeRebuildFlagfile(pathname);
}

void Datafile::reindex()
{
	// Ensure that there are no rebuild or repack flagfiles
	if (hasRebuildFlagfile(pathname))
		throw wibble::exception::Consistency("reindexing " + pathname, "rebuild flagfile exists: perform a rebuild first");

        // Read all the metadata
        string fname = pathname + ".metadata";
        std::ifstream in;
        in.open(fname.c_str(), ios::in);
        if (!in.is_open() || in.fail())
                throw wibble::exception::File(fname, "opening file for reading");

	metadata::Collector mds;
	vector<size_t> offsets;
        while (true)
        {
                size_t offset = in.tellg();
		Metadata md;
                if (!md.read(in, fname))
                        break;
                // Skip deleted metadata
                if (md.deleted) continue;
		mds.push_back(md);
		offsets.push_back(offset);
        }

        in.close();

	// Scan the list of metadata, looking for duplicates and marking all
	// the duplicates except the last one as deleted
	map<string, Metadata*> finddupes;
	for (metadata::Collector::iterator i = mds.begin(); i != mds.end(); ++i)
	{
		string id = parent->id(*i);
		if (id.empty())
			continue;
		map<string, Metadata*>::iterator dup = finddupes.find(id);
		if (dup == finddupes.end())
			finddupes.insert(make_pair(id, &(*i)));
		else
		{
			dup->second->deleted = true;
			dup->second = &(*i);
		}
	}

	// Reindex all the metadata
	for (size_t i = 0; i < mds.size(); ++i)
	{
		// Skip deleted metadata
		if (mds[i].deleted)
		{
			auto_ptr<NormalAccess> na(new NormalAccess(pathname));
			na->remove(offsets[i]);
		} else {
			try {
				parent->addToIndex(mds[i], relname, offsets[i]);
			} catch (utils::sqlite::DuplicateInsert) {
				// If we end up here, it means that we have a
				// duplicate stored in a different file
				throw wibble::exception::Consistency("repacking " + pathname,
					"data item at offset " + str::fmt(offsets[i]) + " has a duplicate elsewhere in the dataset: manual fix is required");
			}
		}
	}
}

}
}
}
}
// vim:set ts=4 sw=4:
