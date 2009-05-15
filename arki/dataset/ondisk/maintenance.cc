/*
 * dataset/ondisk/writer - Local on disk dataset writer
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

#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/writer.h>

#include <ostream>


using namespace std;
using namespace arki::dataset::ondisk::maint;

namespace arki {
namespace dataset {
namespace ondisk {

MaintenanceReport::MaintenanceReport(std::ostream& log)
	: log(log), rebuild(0), fileSummary(0), fileReindex(0), dirSummary(0), index(0), needFullIndex(false), writer(0) {}

void MaintenanceReport::needsDatafileRebuild(dataset::ondisk::maint::Datafile& df)
{
	++rebuild;
	log << df.pathname << ": needs metadata rebuild." << endl;
}
void MaintenanceReport::needsSummaryRebuild(dataset::ondisk::maint::Datafile& df)
{
	++fileSummary;
	log << df.pathname << ": needs summary rebuild." << endl;
}
void MaintenanceReport::needsReindex(dataset::ondisk::maint::Datafile& df)
{
	++fileReindex;
	log << df.pathname << ": needs reindexing." << endl;
}
void MaintenanceReport::needsSummaryRebuild(dataset::ondisk::maint::Directory& df)
{
	++dirSummary;
	log << df.fullpath() << ": needs summary rebuild." << endl;
}
void MaintenanceReport::needsFullIndexRebuild(dataset::ondisk::maint::RootDirectory& df)
{
	needFullIndex = true;
	log << writer->path() << ": needs full index rebuild." << endl;
}
void MaintenanceReport::needsIndexRebuild(dataset::ondisk::maint::Datafile& df)
{
	if (!needFullIndex)
	{
		++index;
		log << df.pathname << ": needs index rebuild." << endl;
	}
}

void MaintenanceReport::report()
{
	bool done = false;
	log << "Summary:";
	if (rebuild)
	{
		done = true;
		log << endl << " " << rebuild << " metadata " << stfile(rebuild) << " to rebuild";
	}
	if (fileSummary)
	{
		done = true;
		log << endl << " " << fileSummary << " data file " << stsumm(fileSummary) << " to rebuild";
	}
	if (fileReindex)
	{
		done = true;
		log << endl << " " << fileReindex << " metadata " << stfile(fileReindex) << " to reindex";
	}
	if (dirSummary)
	{
		done = true;
		log << endl << " " << dirSummary << " directory " << stsumm(dirSummary) << " to rebuild";
	}
	if (needFullIndex)
	{
		done = true;
		log << endl << " the entire index needs to be rebuilt";
	}
	if (index)
	{
		done = true;
		log << endl << " " << index << " index " << stfile(index) << " to reindex";
	}
	if (!done)
		log << " nothing to do." << endl;
	else
		log << "; rerun with -f to fix." << endl;
}


FullMaintenance::FullMaintenance(std::ostream& log, MetadataConsumer& salvage)
	: log(log), salvage(salvage), reindexAll(false), writer(0)
{
}

FullMaintenance::~FullMaintenance()
{
}

void FullMaintenance::start(Writer& w)
{
	writer = &w;
	reindexAll = true;
}
void FullMaintenance::end()
{
	if (writer)
	{
		writer->flush();
		writer = 0;
	}
}

void FullMaintenance::needsDatafileRebuild(Datafile& df)
{
	// Run the rebuild
	log << df.pathname << ": rebuilding metadata." << endl;
	df.rebuild(salvage, reindexAll);
}

void FullMaintenance::needsSummaryRebuild(Datafile& df)
{
	log << df.pathname << ": rebuilding summary." << endl;
	df.rebuildSummary(reindexAll);
}

void FullMaintenance::needsReindex(maint::Datafile& df)
{
	log << df.pathname << ": reindexing data." << endl;
	df.reindex(salvage);
}

void FullMaintenance::needsSummaryRebuild(Directory& df)
{
	log << df.fullpath() << ": rebuilding summary." << endl;
	df.rebuildSummary();
}

void FullMaintenance::needsFullIndexRebuild(RootDirectory& df)
{
	if (writer)
	{
		log << writer->path() << ": fully rebuilding index." << endl;
		// Delete the index and recreate it new
		df.recreateIndex();
		// Delete all summary files
		df.deleteSummaries();
		// Set a flag saying that we need to reindex every file
		reindexAll = true;
	}
}


FullRepack::FullRepack(std::ostream& log)
	: log(log), writer(0)
{
}

FullRepack::~FullRepack()
{
}

void FullRepack::start(Writer& w)
{
	writer = &w;
}
void FullRepack::end()
{
	writer = 0;
}

void FullRepack::needsRepack(Datafile& df)
{
	// Run the rebuild
	log << df.pathname << ": repacking." << endl;
	df.repack();
	// TODO: Reindex
}

void RepackReport::needsRepack(Datafile& df)
{
	++repack;
	log << "Needs repack:" << df.pathname << endl;
}

void RepackReport::end()
{
	bool done = false;
	log << "Summary:";
	if (repack)
	{
		done = true;
		log << endl << " " << repack << " metadata " << stfile(repack) << " to repack";
	}
	if (!done)
		log << " nothing to do." << endl;
	else
		log << "; rerun with -f to repack." << endl;
}

}
}
}
// vim:set ts=4 sw=4:
