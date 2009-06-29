#ifndef ARKI_DATASET_ONDISK_MAINT_DIRECTORY_H
#define ARKI_DATASET_ONDISK_MAINT_DIRECTORY_H

/*
 * dataset/ondisk/maint/directory - Local on disk dataset directory
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

#include <arki/dataset/ondisk/index.h>

#include <string>
#include <vector>
#include <map>
#include <memory>

namespace arki {
class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {
namespace ondisk {
class MaintenanceAgent;
class RepackAgent;
class Visitor;

namespace maint {
class Datafile;

/**
 * Manage a directory in the Ondisk dataset
 */
class Directory
{
protected:
	virtual ~Directory();

public:
	/// Return the full path to this directory
	virtual std::string fullpath() const = 0;

	/// Return the path relative to the RootDirectory in the hierarchy
	virtual std::string relpath() const = 0;

	/// Remove all index entries for this file (given the path relative to the dataset root)
	virtual void resetIndex(const std::string& file) = 0;

	/// Add the given metadata to the index
	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs) = 0;

	/**
	 * Compute the ID for a metadata.
	 *
	 * Returns string() if we are not using an index
	 */
	virtual std::string id(const Metadata& md) const { return std::string(); }

	/**
	 * Rebuild the summary by merging all the summaries from subdirectories and
	 * data files.  It does not work recursively, but it is usually called in a
	 * depth-first search, so it can assume that all summaries in the
	 * subdirectories are up to date.
	 */
	virtual void rebuildSummary();

	/**
	 * Remove all index data and create rebuild flagfiles.  This leaves only
	 * the data in the dataset, and a following rebuild will completely
	 * regenerate all dataset metadata from scratch.
	 */
	virtual void invalidateAll();

	/**
	 * Check if index files (index.sqlite and all the summaries) are up to date
	 *
	 * Scans the directory depth-first, calling the agent methods if something
	 * needs doing.
	 */
	virtual void checkDirectories(MaintenanceAgent& a);

	/// Delete all summary files
	virtual void deleteSummaries();

	/**
	 * Iterate through the contents of the directory, in depth-first order.
	 */
	virtual void depthFirstVisit(Visitor& v);

	/**
	 * Reindex the directory contents
	 */
	void reindex(MetadataConsumer& salvage);

	/**
	 * Read the summary info from the file into s.
	 *
	 * If the file does not exist, nothing is read.
	 */
	static void readSummary(Summary& s, const std::string& fname);

	/// Atomically save the summary to the file
	static void writeSummary(const Summary& s, const std::string& fname);
};

class RootDirectory : public Directory
{
protected:
	std::string m_path;
	std::string m_name;

public:
	RootDirectory(const ConfigFile& cfg);
	~RootDirectory();

	virtual std::string fullpath() const;
	virtual std::string relpath() const;

	virtual void resetIndex(const std::string& file);
	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);

	virtual void invalidateAll();

	virtual void commit();

	/**
	 * Repack all the data files in the directory that need repacking
	 *
	 * A data file needs repacking if it has a .needs-pack flagfile and not a
	 * repack flagfile
	 */
	virtual void checkForRepack(RepackAgent& a);

	/**
	 * Check if the main index is usable.
	 *
	 * Returns true if it needs to be rebuilt, false if the main index is ok.
	 */
	virtual bool checkMainIndex(MaintenanceAgent& a);

	/**
	 * Check if data files (.[format] and .[format].metadata) are all usable
	 *
	 * Scans the directory depth-first, calling the agent methods if something
	 * needs doing.
	 *
	 * If full_reindex is true, then the index of a file will be rebuilt, even
	 * if the file is ok.
	 */
	virtual void checkDataFiles(MaintenanceAgent& a, bool full_reindex = false);

	virtual void checkDirectories(MaintenanceAgent& a);

	virtual void recreateIndex();

	static std::auto_ptr<RootDirectory> create(const ConfigFile& cfg);
};

class IndexedRootDirectory : public RootDirectory
{
protected:
	WIndex m_idx;
	Pending m_trans;

	void beginTransactionIfNeeded();

public:
	IndexedRootDirectory(const ConfigFile& cfg);
	~IndexedRootDirectory();

	virtual void resetIndex(const std::string& file);
	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);
	virtual std::string id(const Metadata& md) const;

	virtual void invalidateAll();

	virtual void commit();

	/**
	 * Delete the index and recreate it
	 */
	virtual void recreateIndex();

	virtual void checkForRepack(RepackAgent& a);

	virtual bool checkMainIndex(MaintenanceAgent& a);
	virtual void checkDataFiles(MaintenanceAgent& a, bool full_reindex = false);
	virtual void checkDirectories(MaintenanceAgent& a);
};

class SubDirectory : public Directory
{
protected:
	Directory* parent;
	std::string path;

public:
	/**
	 * Directory rooted at \a parent, with relative path \a path.
	 *
	 * \warning Do not add components like . or .. in \a path: this causes
	 * shared Directory entries to be freed multiple times, with a resulting
	 * crash.  It is safe not to support this, as \a path always comes from
	 * clean sources.
	 */
	SubDirectory(Directory* parent, const std::string& path);

	virtual std::string fullpath() const;
	virtual std::string relpath() const;

	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);
	virtual void resetIndex(const std::string& file);
	virtual std::string id(const Metadata& md) const;
};

}
}
}
}

// vim:set ts=4 sw=4:
#endif
