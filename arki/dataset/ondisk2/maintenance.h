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
class WritableLocal;

namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
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

/**
 * Perform real repacking
 */
struct RealRepacker : public maintenance::Agent
{
	size_t m_count_packed;
	size_t m_count_archived;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_rescanned;
	size_t m_count_freed;
	bool m_touched_archive;
	bool m_redo_summary;

	RealRepacker(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealFixer : public maintenance::Agent
{
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;
	bool m_touched_archive;
	bool m_redo_summary;

	RealFixer(std::ostream& log, WritableLocal& w);

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

	virtual void enterDataset(WritableLocal&) {}
	virtual void leaveDataset(WritableLocal&) {}

	virtual void enterDirectory(maint::Directory& dir) = 0;
	virtual void leaveDirectory(maint::Directory& dir) = 0;

	virtual void visitFile(maint::Datafile& file) = 0;
};

struct MaintenanceAgent
{
	virtual ~MaintenanceAgent() {}

	virtual void start(WritableLocal&) {}
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

	WritableLocal* writer;

public:
	FullMaintenance(std::ostream& log);
	virtual ~FullMaintenance();

	virtual void start(WritableLocal& w);
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

	WritableLocal* writer;

public:
	FullRepack(MetadataConsumer& mdc, std::ostream& log);
	virtual ~FullRepack();

	virtual void start(WritableLocal& w);
	virtual void end();

	virtual void needsRepack(maint::Datafile& df);
};


}
}
}

// vim:set ts=4 sw=4:
#endif
