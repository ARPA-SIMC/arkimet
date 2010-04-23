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
#include <arki/dataset/ondisk2/archive.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/utils/compress.h>
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

FileCopier::FileCopier(WIndex& idx, const std::string& src, const std::string& dst)
	: m_idx(idx), m_val(scan::Validator::by_filename(src)), src(src), dst(dst), fd_src(-1), fd_dst(-1), w_off(0)
{
	fd_src = open(src.c_str(), O_RDONLY | O_NOATIME);
	if (fd_src < 0)
		throw wibble::exception::File(src, "opening file");

	fd_dst = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd_dst < 0)
		throw wibble::exception::File(dst, "opening file");
}

FileCopier::~FileCopier()
{
	flush();
}

void FileCopier::operator()(const std::string& file, int id, off_t offset, size_t size)
{
	if (buf.size() < size)
		buf.resize(size);
	ssize_t res = pread(fd_src, buf.data(), size, offset);
	if (res < 0 || (unsigned)res != size)
		throw wibble::exception::File(src, "reading " + str::fmt(size) + " bytes");
	m_val.validate(buf.data(), size);
	res = write(fd_dst, buf.data(), size);
	if (res < 0 || (unsigned)res != size)
		throw wibble::exception::File(dst, "writing " + str::fmt(size) + " bytes");

	// Reindex file from offset to w_off
	m_idx.relocate_data(id, w_off);

	w_off += size;
}

void FileCopier::flush()
{
	if (fd_src != -1 and close(fd_src) != 0)
		throw wibble::exception::File(src, "closing file");
	fd_src = -1;
	if (fd_dst != -1 and close(fd_dst) != 0)
		throw wibble::exception::File(dst, "closing file");
	fd_dst = -1;
}


// Agent

Agent::Agent(std::ostream& log, Writer& w)
	: m_log(log), w(w), lineStart(true)
{
}

std::ostream& Agent::log()
{
	return m_log << w.m_name << ": ";
}

void Agent::logStart()
{
	lineStart = true;
}

std::ostream& Agent::logAdd()
{
	if (lineStart)
	{
		lineStart = false;
		return m_log << w.m_name << ": ";
	}
	else
		return m_log << ", ";
}

void Agent::logEnd()
{
	if (! lineStart)
		m_log << "." << endl;
}

// FailsafeRepacker

FailsafeRepacker::FailsafeRepacker(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_deleted(0)
{
}

void FailsafeRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_INDEX: ++m_count_deleted; break;
		default: break;
	}
}

void FailsafeRepacker::end()
{
	if (m_count_deleted == 0) return;
	log() << "index is empty, skipping deletion of "
	      << m_count_deleted << " files."
	      << endl;
}

// Common maintenance functions

static size_t repack(const std::string& root, const std::string& file, WIndex& idx)
{
	// Lock away writes and reads
	Pending p = idx.beginExclusiveTransaction();

	// Make a copy of the file with the right data in it, sorted by
	// reftime, and update the offsets in the index
	string pathname = str::joinpath(root, file);
	string pntmp = pathname + ".repack";
	FileCopier copier(idx, pathname, pntmp);
	idx.scan_file(file, copier, "reftime, offset");
	copier.flush();

	size_t size_pre = files::size(pathname);
	size_t size_post = files::size(pntmp);

	// Remove the .metadata file if present, because we are shuffling the
	// data file and it will not be valid anymore
	string mdpathname = pathname + ".metadata";
	if (sys::fs::access(mdpathname, F_OK))
		if (unlink(mdpathname.c_str()) < 0)
			throw wibble::exception::System("removing obsolete metadata file " + mdpathname);

	// Rename the file with to final name
	if (rename(pntmp.c_str(), pathname.c_str()) < 0)
		throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

	// Commit the changes on the database
	p.commit();

	return size_pre - size_post;
}

struct Reindexer : public MetadataConsumer
{
	WIndex& idx;
	std::string dsname;
	std::string relfile;
	std::string basename;

	Reindexer(WIndex& idx,
		  const std::string& dsname,
		  const std::string& relfile)
	       	: idx(idx), dsname(dsname), relfile(relfile), basename(str::basename(relfile)) {}

	virtual bool operator()(Metadata& md)
	{
		if (md.deleted) return true;
		Item<types::source::Blob> blob = md.source.upcast<types::source::Blob>();
		try {
			int id;
			if (str::basename(blob->filename) != basename)
				throw wibble::exception::Consistency(
					"rescanning " + relfile,
				       	"metadata points to the wrong file: " + blob->filename);
			idx.index(md, relfile, blob->offset, &id);
		} catch (utils::sqlite::DuplicateInsert& di) {
			throw wibble::exception::Consistency("reindexing " + basename,
					"data item at offset " + str::fmt(blob->offset) + " has a duplicate elsewhere in the dataset: manual fix is required");
		} catch (std::exception& e) {
			// sqlite will take care of transaction consistency
			throw wibble::exception::Consistency("reindexing " + basename,
					"failed to reindex data item at offset " + str::fmt(blob->offset) + ": " + e.what());
		}
		return true;
	}
};

static void rescan(const std::string& dsname, const std::string& root, const std::string& file, WIndex& idx)
{
	// Lock away writes and reads
	Pending p = idx.beginExclusiveTransaction();
	// cerr << "LOCK" << endl;

	// Remove from the index all data about the file
	idx.reset(file);
	// cerr << " RESET " << file << endl;

	string pathname = str::joinpath(root, file);

	// Temporarily uncompress the file for scanning
	auto_ptr<utils::compress::TempUnzip> tu;
	if (!sys::fs::access(pathname, F_OK) && sys::fs::access(pathname + ".gz", F_OK))
		tu.reset(new utils::compress::TempUnzip(pathname));

	// Collect the scan results in a metadata::Collector
	metadata::Collector mds;
	if (!scan::scan(pathname, mds))
		throw wibble::exception::Consistency("rescanning " + pathname, "file format unknown");
	// cerr << " SCANNED " << pathname << ": " << mds.size() << endl;

	// Scan the list of metadata, looking for duplicates and marking all
	// the duplicates except the last one as deleted
	index::IDMaker id_maker(idx.unique_codes());

	map<string, Metadata*> finddupes;
	for (metadata::Collector::iterator i = mds.begin(); i != mds.end(); ++i)
	{
		string id = id_maker.id(*i);
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
	// cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

	Reindexer fixer(idx, dsname, file);
	bool res = mds.sendTo(fixer);
	assert(res);
	// cerr << " REINDEXED " << pathname << endl;

	// TODO: if scan fails, remove all info from the index and rename the
	// file to something like .broken

	// Commit the changes on the database
	p.commit();
	// cerr << " COMMITTED" << endl;
}

// RealRepacker

RealRepacker::RealRepacker(std::ostream& log, Writer& w)
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
			size_t saved = repack(w.m_path, file, w.m_idx);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			m_count_freed += saved;
			break;
		}
		case TO_ARCHIVE: {
			// Create the target directory in the archive
			string pathname = str::joinpath(w.m_path, file);
			string arcrelname = str::joinpath("last", file);
			string arcabsname = str::joinpath(w.m_path, str::joinpath(".archive", arcrelname));
			sys::fs::mkFilePath(arcabsname);

			// Rebuild the metadata
			metadata::Collector mds;
			w.m_idx.scan_file(file, mds);

			// Remove from index
			w.m_idx.reset(file);

			// Move to archive
			if (sys::fs::access(arcabsname, F_OK))
				throw wibble::exception::Consistency("archiving " + pathname + " to " + arcabsname,
						arcabsname + " already exists");
			if (rename(pathname.c_str(), arcabsname.c_str()) < 0)
				throw wibble::exception::System("moving " + pathname + " to " + arcabsname);

			// Acquire in the achive
			w.archive().acquire(arcrelname, mds);

			log() << "archived " << file << endl;
			++m_count_archived;
			m_touched_archive = true;
			m_redo_summary = true;
			break;
		}
		case TO_DELETE: {
			// Delete obsolete files
			w.m_idx.reset(file);
			string pathname = str::joinpath(w.m_path, file);
			size_t size = files::size(pathname);
			if (unlink(pathname.c_str()) < 0)
				throw wibble::exception::System("removing " + pathname);

			log() << "deleted " << file << " (" << size << " freed)" << endl;
			++m_count_deleted;
			++m_count_deindexed;
			m_count_freed += size;
			m_redo_summary = true;
			break;
		}
		case TO_INDEX: {
			// Delete all files not indexed
			string pathname = str::joinpath(w.m_path, file);
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
			w.m_idx.reset(file);
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
	size_t size_pre = 0, size_post = 0;
	if (files::size(w.m_idx.pathname() + "-journal") > 0)
	{
		size_pre = files::size(w.m_idx.pathname())
				+ files::size(w.m_idx.pathname() + "-journal");
		w.m_idx.vacuum();
		size_post = files::size(w.m_idx.pathname())
				+ files::size(w.m_idx.pathname() + "-journal");
		if (size_pre != size_post)
			log() << "database cleaned up" << endl;
	}

	// Rebuild the cached summary
	if (m_redo_summary ||
	    !sys::fs::access(str::joinpath(w.m_path, ".summaries/all.summary"), F_OK))
	{
		w.m_idx.rebuildSummaryCache();
		log() << "rebuild summary cache" << endl;
	}

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
	if (size_pre > size_post)
	{
		logAdd() << (size_pre - size_post) << " bytes reclaimed on the index";
		m_count_freed += (size_pre - size_post);
	}
	if (m_count_freed > 0)
		logAdd() << m_count_freed << " total bytes freed";
	logEnd();
}

// MockRepacker

MockRepacker::MockRepacker(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_packed(0), m_count_archived(0),
	  m_count_deleted(0), m_count_deindexed(0), m_count_rescanned(0)
{
}

void MockRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK:
			log() << file << " should be packed" << endl;
			++m_count_packed;
			break;
		case TO_ARCHIVE:
			log() << file << " should be archived" << endl;
			++m_count_archived;
			break;
		case TO_DELETE:
			log() << file << " should be deleted and removed from the index" << endl;
			++m_count_deleted;
			++m_count_deindexed;
			break;
		case TO_INDEX:
			log() << file << " should be deleted" << endl;
			++m_count_deleted;
			break;
		case DELETED:
			log() << file << " should be removed from the index" << endl;
			++m_count_deindexed;
			break;
		case ARC_TO_INDEX:
		case ARC_TO_RESCAN: {
			log() << file << " should be rescanned by the archive" << endl;
			++m_count_rescanned;
			break;
		}
		case ARC_DELETED: {
			log() << file << " should be removed from the archive index" << endl;
			++m_count_deindexed;
			break;
		}
		default:
			break;
	}
}

void MockRepacker::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " should be packed";
	if (m_count_archived)
		logAdd() << nfiles(m_count_archived) << " should be archived";
	if (m_count_deleted)
		logAdd() << nfiles(m_count_deleted) << " should be deleted";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " should be removed from the index";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " should be rescanned";
	logEnd();
}

// RealFixer

RealFixer::RealFixer(std::ostream& log, Writer& w)
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
			size_t saved = repack(w.m_path, file, w.m_idx);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			break;
		}
		*/
		case TO_INDEX:
		case TO_RESCAN: {
			rescan(w.m_name, w.m_path, file, w.m_idx);
			log() << "rescanned " << file << endl;
			++m_count_rescanned;
			m_redo_summary = true;
			break;
		}
		case DELETED: {
			// Remove from index those files that have been deleted
			w.m_idx.reset(file);
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
	size_t size_pre = 0, size_post = 0;
	if (files::size(w.m_idx.pathname() + "-journal") > 0)
	{
		size_pre = files::size(w.m_idx.pathname())
				+ files::size(w.m_idx.pathname() + "-journal");
		w.m_idx.vacuum();
		size_post = files::size(w.m_idx.pathname())
				+ files::size(w.m_idx.pathname() + "-journal");
		if (size_pre != size_post)
			log() << "database cleaned up" << endl;
	}

	// Rebuild the cached summary
	if (m_redo_summary ||
	    !sys::fs::access(str::joinpath(w.m_path, ".summaries/all.summary"), F_OK))
	{
		w.m_idx.rebuildSummaryCache();
		log() << "rebuild summary cache" << endl;
	}

	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " packed";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " rescanned";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " removed from index";
	if (size_pre > size_post)
		logAdd() << (size_pre - size_post) << " bytes reclaimed cleaning the index";
	logEnd();
}

// MockFixer

MockFixer::MockFixer(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0)
{
}

void MockFixer::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK:
			log() << file << " should be packed" << endl;
			++m_count_packed;
			break;
		case TO_INDEX:
		case TO_RESCAN:
			log() << file << " should be rescanned" << endl;
			++m_count_rescanned;
			break;
		case DELETED:
			log() << file << " should be removed from the index" << endl;
			++m_count_deindexed;
			break;
		case ARC_TO_INDEX:
		case ARC_TO_RESCAN: {
			log() << file << " should be rescanned by the archive" << endl;
			++m_count_rescanned;
			break;
		}
		case ARC_DELETED: {
			log() << file << " should be removed from the archive index" << endl;
			++m_count_deindexed;
			break;
		}
		default:
			break;
	}
}

void MockFixer::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " should be packed";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " should be rescanned";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " should be removed from the index";
	logEnd();
}

}

}
}
}
// vim:set ts=4 sw=4:
