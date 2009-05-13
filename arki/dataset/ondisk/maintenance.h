#ifndef ARKI_DATASET_ONDISK_MAINTENANCE_H
#define ARKI_DATASET_ONDISK_MAINTENANCE_H

/*
 * dataset/ondisk/maintenance - Ondisk dataset maintenance
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

#include <iosfwd>

namespace arki {
class MetadataConsumer;

namespace dataset {
namespace ondisk {
class Writer;

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
	virtual void end() {}
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
	// This is where repack progess will be logged
	std::ostream& log;

	Writer* writer;

public:
	FullRepack(std::ostream& log);
	virtual ~FullRepack();

	virtual void start(Writer& w);
	virtual void end();

	virtual void needsRepack(maint::Datafile& df);
};

/**
 * RepackAgent that only prints information on what repack would do
 */
struct RepackReport : public RepackAgent
{
	std::ostream& log;
	size_t repack;

	RepackReport(std::ostream& log) : log(log), repack(0) {}
	virtual ~RepackReport() {}

	const char* stfile(size_t count) const { return count > 1 ? "files" : "file"; }

	virtual void needsRepack(maint::Datafile& df);
	virtual void end();
};


}
}
}

// vim:set ts=4 sw=4:
#endif
