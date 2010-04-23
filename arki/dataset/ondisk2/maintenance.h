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

#include <arki/dataset/maintenance.h>
#include <wibble/sys/buffer.h>
#include <iosfwd>

namespace arki {
class MetadataConsumer;

namespace scan {
struct Validator;
}

namespace dataset {

namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
class Writer;
class WIndex;
class Index;

namespace writer {

struct CheckAge : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;
	const Index& idx;
	std::string archive_threshold;
	std::string delete_threshold;

	CheckAge(MaintFileVisitor& next, const Index& idx, int archive_age=-1, int delete_age=-1);

	void operator()(const std::string& file, State state);
};

struct FileCopier : maintenance::IndexFileVisitor
{
	WIndex& m_idx;
	const scan::Validator& m_val;
	wibble::sys::Buffer buf;
	std::string src;
	std::string dst;
	int fd_src;
	int fd_dst;
	off_t w_off;

	FileCopier(WIndex& idx, const std::string& src, const std::string& dst);
	virtual ~FileCopier();

	void operator()(const std::string& file, int id, off_t offset, size_t size);

	void flush();
};

/// Base class for all repackers and rebuilders
struct Agent : public maintenance::MaintFileVisitor
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
	size_t m_count_archived;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_rescanned;
	size_t m_count_freed;
	bool m_touched_archive;
	bool m_redo_summary;

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
	size_t m_count_archived;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_rescanned;

	MockRepacker(std::ostream& log, Writer& w);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealFixer : public Agent
{
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;
	bool m_touched_archive;
	bool m_redo_summary;

	RealFixer(std::ostream& log, Writer& w);

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
	bool reindexAll;

	Writer* writer;

public:
	FullMaintenance(std::ostream& log);
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
