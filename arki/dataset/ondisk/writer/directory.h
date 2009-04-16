#ifndef ARKI_DATASET_ONDISK_WRITER_DIRECTORY_H
#define ARKI_DATASET_ONDISK_WRITER_DIRECTORY_H

/*
 * dataset/ondisk/writer/directory - Local on disk dataset directory
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

#include <arki/dataset/ondisk/index.h>

#include <string>
#include <vector>
#include <map>

namespace wibble {
namespace sys {
namespace fs {
class Lockfile;
}
}
}

namespace arki {

class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {
class TargetFile;

namespace ondisk {
class MaintenanceAgent;
class RepackAgent;
class Visitor;

namespace writer {
class Datafile;

/**
 * Manage a directory in the Ondisk dataset
 */
class Directory
{
protected:
	std::map<std::string, Directory*> m_subdir_cache;
	std::map<std::string, Datafile*> m_files_cache;
	Summary* m_summary;
	bool summaryIsInvalid;

	/// Remove those files and subdirs from the cache that don't need flushing
	void releaseUnused();

	virtual ~Directory();
	Directory();

public:
	/// Return a (shared) instance of the Directory for the given relative pathname
	Directory* subdir(const std::string& path);

	/// Return a (shared) instance of the Datafile for the given relative pathname
	Datafile* file(const std::string& path);

	/// Return the full path to this directory
	virtual std::string fullpath() const = 0;

	/// Return the path relative to the RootDirectory in the hierarchy
	virtual std::string relpath() const = 0;

	/// Get the summary for this directory
	const Summary& summary();

	/// Add the given metadata to the summary
	virtual void addToSummary(Metadata& md);

	/// Add the given metadata to the index
	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs) = 0;

	/**
	 * Flush things at the end of updates.
	 *
	 * What is does is:
	 *  \l call done() on all open Datafiles
	 *  \l write all the changed summaries to disk
	 *  \l empty the shared instance caches
	 */
	virtual void flush();

	/**
	 * Invalidate the summary, so that that it will be rebuild by merging its
	 * sources again.
	 */
	virtual void invalidateSummary();

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

	TargetFile* m_tf;
	wibble::sys::fs::Lockfile* m_lockfile;

public:
	RootDirectory(const ConfigFile& cfg);
	~RootDirectory();

	virtual std::string id(const Metadata& md) const;

	virtual std::string fullpath() const;
	virtual std::string relpath() const;

	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);

	virtual void acquire(Metadata& md);
	virtual void replace(Metadata& md);
	virtual void remove(const std::string& id);

	virtual void flush();

	static void testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
	static void testReplace(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
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

	virtual std::string id(const Metadata& md) const;

	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);

	virtual void acquire(Metadata& md);
	virtual void replace(Metadata& md);
	virtual void remove(const std::string& id);

	virtual void flush();

	static void testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
	static void testReplace(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
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

	virtual void addToSummary(Metadata& md);
	virtual void addToIndex(Metadata& md, const std::string& file, size_t ofs);

	virtual void invalidateSummary();
};

}
}
}
}

// vim:set ts=4 sw=4:
#endif
