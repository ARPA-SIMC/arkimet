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
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/dataset/archive.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/scan/any.h>
#include <arki/nag.h>

#include <wibble/sys/fs.h>

#include <ostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <cstdio>

// Be compatible with systems without O_NOATIME
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;
using namespace arki::dataset::ondisk2::maint;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

template<typename T>
static std::string nfiles(const T& val)
{
	if (val == 1)
		return "1 file";
	else
		return str::fmt(val) + " files";
}

CheckAge::CheckAge(MaintFileVisitor& next, const Index& idx, int archive_age, int delete_age)
	: next(next), idx(idx)
{
	time_t now = time(NULL);
	struct tm t;

	// Go to the beginning of the day
	now -= (now % (3600*24));

	if (archive_age != -1)
	{
		time_t arc_thr = now - archive_age * 3600 * 24;
		gmtime_r(&arc_thr, &t);
		archive_threshold = str::fmtf("%04d-%02d-%02d %02d:%02d:%02d",
				t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
				t.tm_hour, t.tm_min, t.tm_sec);
	}
	if (delete_age != -1)
	{
		time_t del_thr = now - delete_age * 3600 * 24;
		gmtime_r(&del_thr, &t);
		delete_threshold = str::fmtf("%04d-%02d-%02d %02d:%02d:%02d",
				t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
				t.tm_hour, t.tm_min, t.tm_sec);
	}
}

void CheckAge::operator()(const std::string& file, State state)
{
	if (state != OK or (archive_threshold.empty() and delete_threshold.empty()))
		next(file, state);
	else
	{
		string maxdate = idx.max_file_reftime(file);
		//cerr << "TEST " << maxdate << " WITH " << delete_threshold << " AND " << archive_threshold << endl;
		if (delete_threshold > maxdate)
		{
			nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
			next(file, TO_DELETE);
		}
		else if (archive_threshold > maxdate)
		{
			nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
			next(file, TO_ARCHIVE);
		}
		else
			next(file, state);
	}
}



// RealRepacker

RealRepacker::RealRepacker(std::ostream& log, WritableLocal& w)
	: Agent(log, w), m_count_packed(0), m_count_archived(0),
	  m_count_deleted(0), m_count_deindexed(0), m_count_rescanned(0),
	  m_count_freed(0), m_touched_archive(false), m_redo_summary(false)
{
}

void RealRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK: {
		        // Repack the file
			size_t saved = w.repackFile(file);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			m_count_freed += saved;
			break;
		}
		case TO_ARCHIVE: {
			// Create the target directory in the archive
			w.archiveFile(file);
			log() << "archived " << file << endl;
			++m_count_archived;
			m_touched_archive = true;
			m_redo_summary = true;
			break;
		}
		case TO_DELETE: {
			// Delete obsolete files
			size_t size = w.removeFile(file, true);
			log() << "deleted " << file << " (" << size << " freed)" << endl;
			++m_count_deleted;
			++m_count_deindexed;
			m_count_freed += size;
			m_redo_summary = true;
			break;
		}
		case TO_INDEX: {
			// Delete all files not indexed
			string pathname = str::joinpath(w.path(), file);
			size_t size = files::size(pathname);
			if (unlink(pathname.c_str()) < 0)
				throw wibble::exception::System("removing " + pathname);

			log() << "deleted " << file << " (" << size << " freed)" << endl;
			++m_count_deleted;
			m_count_freed += size;
			break;
		}
		case DELETED: {
			// Remove from index those files that have been deleted
			w.removeFile(file, false);
			log() << "deleted from index " << file << endl;
			++m_count_deindexed;
			m_redo_summary = true;
			break;
	        }
		case ARC_TO_INDEX:
		case ARC_TO_RESCAN: {
			/// File is not present in the archive index
			/// File contents need reindexing in the archive
			w.archive().rescan(file);
			log() << "rescanned in archive " << file << endl;
			++m_count_rescanned;
			m_touched_archive = true;
			break;
		}
		case ARC_DELETED: {
			w.archive().remove(file);
			log() << "deleted from archive index " << file << endl;
			++m_count_deindexed;
			m_touched_archive = true;
			break;
		}
		default:
			break;
	}
}

void RealRepacker::end()
{
	if (m_touched_archive)
	{
		w.archive().vacuum();
		log() << "archive cleaned up" << endl;
	}

	// Finally, tidy up the database
	size_t size_vacuum = w.vacuum();

	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " packed";
	if (m_count_archived)
		logAdd() << nfiles(m_count_archived) << " archived";
	if (m_count_deleted)
		logAdd() << nfiles(m_count_deleted) << " deleted";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " removed from index";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " rescanned";
	if (size_vacuum)
	{
		logAdd() << size_vacuum << " bytes reclaimed on the index";
		m_count_freed += size_vacuum;
	}
	if (m_count_freed > 0)
		logAdd() << m_count_freed << " total bytes freed";
	logEnd();
}

// RealFixer

RealFixer::RealFixer(std::ostream& log, WritableLocal& w)
	: Agent(log, w), 
          m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0),
	  m_touched_archive(false), m_redo_summary(false)
{
}

void RealFixer::operator()(const std::string& file, State state)
{
	switch (state)
	{
		/* Packing is left to the repacker, during check we do not
		 * mangle the data files
		case TO_PACK: {
		        // Repack the file
			size_t saved = repack(w.path(), file, w.m_idx);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			break;
		}
		*/
		case TO_INDEX:
		case TO_RESCAN: {
			w.rescanFile(file);
			log() << "rescanned " << file << endl;
			++m_count_rescanned;
			m_redo_summary = true;
			break;
		}
		case DELETED: {
			// Remove from index those files that have been deleted
			w.removeFile(file, false);
			log() << "deindexed " << file << endl;
			++m_count_deindexed;
			m_redo_summary = true;
			break;
	        }
		case ARC_TO_INDEX:
		case ARC_TO_RESCAN: {
			/// File is not present in the archive index
			/// File contents need reindexing in the archive
			w.archive().rescan(file);
			log() << "rescanned in archive " << file << endl;
			++m_count_rescanned;
			m_touched_archive = true;
			break;
		}
		case ARC_DELETED: {
			/// File does not exist, but has entries in the archive index
			w.archive().remove(file);
			log() << "deindexed in archive " << file << endl;
			++m_count_deindexed;
			m_touched_archive = true;
			break;
		}
		default:
			break;
	}
}

void RealFixer::end()
{
	if (m_touched_archive)
	{
		w.archive().vacuum();
		log() << "archive cleaned up" << endl;
	}

	// Finally, tidy up the database
	size_t size_vacuum = w.vacuum();

	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " packed";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " rescanned";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " removed from index";
	if (size_vacuum)
		logAdd() << size_vacuum << " bytes reclaimed cleaning the index";
	logEnd();
}

}
}
}
}
// vim:set ts=4 sw=4:
