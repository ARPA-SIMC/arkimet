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

#include <arki/dataset/ondisk2/maintenance.h>
//#include <arki/dataset/ondisk2/maint/datafile.h>
//#include <arki/dataset/ondisk2/maint/directory.h>
#include <arki/dataset/ondisk2/writer.h>

#include <ostream>


using namespace std;
using namespace arki::dataset::ondisk2::maint;

namespace arki {
namespace dataset {
namespace ondisk2 {

#if 0
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


FullRepack::FullRepack(MetadataConsumer& mdc, std::ostream& log)
	: mdc(mdc), log(log), writer(0)
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
	df.repack(mdc);
	// TODO: Reindex
}
#endif

}
}
}
// vim:set ts=4 sw=4:
