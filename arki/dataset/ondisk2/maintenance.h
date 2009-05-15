#ifndef ARKI_DATASET_ONDISK2_MAINTENANCE_H
#define ARKI_DATASET_ONDISK2_MAINTENANCE_H

/*
 * dataset/ondisk2/maintenance - Ondisk2 dataset maintenance
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

#include <arki/dataset/ondisk2/writer/utils.h>
#include <wibble/sys/buffer.h>
#include <iosfwd>

namespace arki {
class MetadataConsumer;

namespace dataset {
namespace ondisk2 {
class Writer;
class WIndex;

namespace writer {

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct MaintFileVisitor
{
	enum State {
		OK,		/// File fully and consistently indexed
		TO_PACK,	/// File contains data that has been deleted
		TO_INDEX,	/// File is not present in the index
		TO_RESCAN,	/// File contents are inconsistent with the index
		DELETED,	/// File does not exist but has entries in the index
	};

	virtual ~MaintFileVisitor() {}

	virtual void operator()(const std::string& file, State state) = 0;
};

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct IndexFileVisitor
{
	virtual ~IndexFileVisitor() {}

	virtual void operator()(const std::string& file, off_t offset, size_t size) = 0;
};

/**
 * IndexFileVisitor that feeds a MaintFileVisitor with OK or HOLES status.
 *
 * TO_INDEX and TO_RESCAN will have to be detected by MaintFileVisitors
 * further down the chain.
 */
struct HoleFinder : IndexFileVisitor
{
	writer::MaintFileVisitor& next;

	const std::string& m_root;

	std::string last_file;
	off_t last_file_size;
	bool has_hole;

	HoleFinder(writer::MaintFileVisitor& next, const std::string& root)
	       	: next(next), m_root(root), has_hole(false) {}

	void finaliseFile();

	void operator()(const std::string& file, off_t offset, size_t size)
	{
		if (last_file != file)
		{
			finaliseFile();
			last_file = file;
			last_file_size = 0;
			has_hole = false;
		}
		if (offset != last_file_size)
			has_hole = true;
		last_file_size += size;
	}

	void end()
	{
		finaliseFile();
	}
};

/**
 * MaintFileVisitor that feeds a MaintFileVisitor with TO_INDEX status.
 *
 * The input feed is assumed to come from the index, and is checked against the
 * files found on disk in order to detect files that are on disk but not in the
 * index.
 */
struct FindMissing : public writer::MaintFileVisitor
{
	writer::MaintFileVisitor& next;
	writer::DirScanner disk;

	FindMissing(MaintFileVisitor& next, const std::string& root) : next(next), disk(root) {}

	// disk:  a, b, c, e, f, g
	// index:       c, d, e, f, g

	void operator()(const std::string& file, State state)
	{
		while (not disk.cur().empty() and disk.cur() < file)
		{
			next(disk.cur(), TO_INDEX);
			disk.next();
		}
		if (disk.cur() == file)
		{
			// TODO: if requested, check for internal consistency
			// TODO: it requires to have an infrastructure for quick
			// TODO:   consistency checkers (like, "GRIB starts with GRIB
			// TODO:   and ends with 7777")

			disk.next();
			next(file, state);
		}
		else // if (disk.cur() > file)
			next(file, DELETED);
	}

	void end()
	{
		while (not disk.cur().empty())
		{
			next(disk.cur(), TO_INDEX);
			disk.next();
		}
	}
};

struct FileCopier : writer::IndexFileVisitor
{
	WIndex& m_idx;
	wibble::sys::Buffer buf;
	std::string src;
	std::string dst;
	int fd_src;
	int fd_dst;
	off_t w_off;

	FileCopier(WIndex& idx, const std::string& src, const std::string& dst);
	virtual ~FileCopier();

	void operator()(const std::string& file, off_t offset, size_t size);

	void flush();
};

struct MaintPrinter : public writer::MaintFileVisitor
{
	void operator()(const std::string& file, State state);
};

/// Base class for all repackers and rebuilders
struct Agent : public writer::MaintFileVisitor
{
	std::ostream& m_log;
	Writer& w;
	bool lineStart;

	Agent(std::ostream& log, Writer& w);

	std::ostream& log();

	// Start a line with multiple items logged
	void logStart();
	// Log another item on the current line
	std::ostream& logAdd();
	// End the line with multiple things logged
	void logEnd();

	virtual void end() {}
};

/**
 * Repacker used when some failsafe is triggered.
 * 
 * It only reports how many files would be deleted.
 */
struct FailsafeRepacker : public Agent
{
	size_t m_count_deleted;

	FailsafeRepacker(std::ostream& log, Writer& w);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealRepacker : public Agent
{
	size_t m_count_packed;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_freed;

	RealRepacker(std::ostream& log, Writer& w);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockRepacker : public Agent
{
	size_t m_count_packed;
	size_t m_count_deleted;
	size_t m_count_deindexed;

	MockRepacker(std::ostream& log, Writer& w);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealFixer : public Agent
{
	MetadataConsumer& salvage;
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;
	size_t m_count_salvaged;

	RealFixer(std::ostream& log, Writer& w, MetadataConsumer& salvage);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockFixer : public Agent
{
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;

	MockFixer(std::ostream& log, Writer& w);

	void operator()(const std::string& file, State state);
	void end();
};


}

namespace maint {
class RootDirectory;
class Directory;
class Datafile;
}

/**
 * Generic dataset visitor interface
 */
struct Visitor
{
	virtual ~Visitor() {}

	virtual void enterDataset(Writer&) {}
	virtual void leaveDataset(Writer&) {}

	virtual void enterDirectory(maint::Directory& dir) = 0;
	virtual void leaveDirectory(maint::Directory& dir) = 0;

	virtual void visitFile(maint::Datafile& file) = 0;
};

struct MaintenanceAgent
{
	virtual ~MaintenanceAgent() {}

	virtual void start(Writer&) {}
	virtual void end() {}

	virtual void needsDatafileRebuild(maint::Datafile& df) = 0;
	virtual void needsSummaryRebuild(maint::Datafile& df) = 0;
	virtual void needsReindex(maint::Datafile& df) = 0;
	virtual void needsSummaryRebuild(maint::Directory& df) = 0;
	virtual void needsFullIndexRebuild(maint::RootDirectory& df) = 0;
};

struct RepackAgent
{
	virtual ~RepackAgent() {}

	virtual void needsRepack(maint::Datafile& df) = 0;
};

/**
 * Full maintenance implementation, trying to solve all the issues that are found
 */
class FullMaintenance : public MaintenanceAgent
{
protected:
	// This is where maintenance progess will be logged
	std::ostream& log;
	MetadataConsumer& salvage;
	bool reindexAll;

	Writer* writer;

public:
	FullMaintenance(std::ostream& log, MetadataConsumer& salvage);
	virtual ~FullMaintenance();

	virtual void start(Writer& w);
	virtual void end();

	virtual void needsDatafileRebuild(maint::Datafile& df);
	virtual void needsSummaryRebuild(maint::Datafile& df);
	virtual void needsReindex(maint::Datafile& df);
	virtual void needsSummaryRebuild(maint::Directory& df);
	virtual void needsFullIndexRebuild(maint::RootDirectory& df);
};

/**
 * Full repack implementation
 */
class FullRepack : public RepackAgent
{
protected:
	// This is where the metadata with inline data of the deleted data is sent
	MetadataConsumer& mdc;

	// This is where repack progess will be logged
	std::ostream& log;

	Writer* writer;

public:
	FullRepack(MetadataConsumer& mdc, std::ostream& log);
	virtual ~FullRepack();

	virtual void start(Writer& w);
	virtual void end();

	virtual void needsRepack(maint::Datafile& df);
};


}
}
}

// vim:set ts=4 sw=4:
#endif
