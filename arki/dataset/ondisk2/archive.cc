/*
 * dataset/ondisk2/archive - Handle archived data
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk2/archive.h>
#include <arki/dataset/ondisk2/writer/utils.h>
#include <arki/dataset/ondisk2/maintenance.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <time.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;

namespace arki {
namespace dataset {
namespace ondisk2 {

Archive::Archive(const std::string& dir, int delete_age)
	: m_dir(dir), m_delete_age(delete_age), m_insert(m_db)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);
}

Archive::~Archive()
{
}

void Archive::openRO()
{
	string pathname = str::joinpath(m_dir, "index.sqlite");
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

	if (!wibble::sys::fs::access(pathname, F_OK))
		throw wibble::exception::Consistency("opening archive index", "index " + pathname + " does not exist");
	
	m_db.open(pathname);
	setupPragmas();
	
	initQueries();
}

void Archive::openRW()
{
	string pathname = str::joinpath(m_dir, "index.sqlite");
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

	bool need_create = !wibble::sys::fs::access(pathname, F_OK);

	m_db.open(pathname);
	setupPragmas();
	
	if (need_create)
		initDB();

	initQueries();
}

void Archive::setupPragmas()
{
	// Also, since the way we do inserts cause no trouble if a reader reads a
	// partial insert, we do not need read locking
	m_db.exec("PRAGMA read_uncommitted = 1");
	// Use new features, if we write we read it, so we do not need to
	// support sqlite < 3.3.0 if we are above that version
	m_db.exec("PRAGMA legacy_file_format = 0");
}

void Archive::initQueries()
{
	m_insert.compile("INSERT INTO files (file, mtime, start_time, end_time) VALUES (?, ?, ?, ?)");
}

void Archive::initDB()
{
	// Create the main table
	string query = "CREATE TABLE IF NOT EXISTS files ("
		"id INTEGER PRIMARY KEY,"
		" file TEXT NOT NULL,"
		" mtime INTEGER NOT NULL,"
		" start_time TEXT NOT NULL,"
		" end_time TEXT NOT NULL,"
		" UNIQUE(file) )";
	m_db.exec(query);
	m_db.exec("CREATE INDEX idx_files_start ON files (start_time)");
	m_db.exec("CREATE INDEX idx_files_end ON files (end_time)");
}

void Archive::fileList(const Matcher& matcher, std::vector<std::string>& files) const
{
	string query;
	const matcher::OR* reftime = 0;
	
	if (matcher.m_impl)
		reftime = matcher.m_impl->get(types::TYPE_REFTIME);

	if (reftime)
	{
		UItem<types::Time> begin;
		UItem<types::Time> end;
		for (matcher::OR::const_iterator j = reftime->begin(); j != reftime->end(); ++j)
		{
			if (j == reftime->begin())
				(*j)->upcast<matcher::MatchReftime>()->dateRange(begin, end);
			else {
				UItem<types::Time> new_begin;
				UItem<types::Time> new_end;
				(*j)->upcast<matcher::MatchReftime>()->dateRange(new_begin, new_end);
				if (begin.defined() && (!new_begin.defined() || new_begin < begin))
					begin = new_begin;
				if (end.defined() && (!new_end.defined() || end < new_end))
					end = new_end;

			}
		}
		query = "SELECT file FROM files";

		if (begin.defined())
			query += " WHERE end_time >= '" + begin->toSQL() + "'";
		if (end.defined())
		{
			if (begin.defined())
				query += " AND start_time <= '" + end->toSQL() + "'";
			else
				query += " WHERE start_time <= '" + end->toSQL() + "'";
		}

		query += " ORDER BY file";
	} else {
		// No restrictions on reftime: get all files
		query = "SELECT file FROM files ORDER BY file";
	}

	// cerr << "Query: " << query << endl;
	
	Query q("sel_archive", m_db);
	q.compile(query);
	while (q.step())
		files.push_back(q.fetchString(0));
}

void Archive::queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer)
{
	vector<string> files;
	fileList(matcher, files);

	// TODO: does it make sense to check with the summary first?

	if (withData)
	{
		ds::DataInliner inliner(consumer);
		ds::PathPrepender prepender(sys::fs::abspath(m_dir), inliner);
		ds::MatcherFilter filter(matcher, prepender);
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			scan::scan(str::joinpath(m_dir, *i), filter);
	} else {
		ds::PathPrepender prepender(sys::fs::abspath(m_dir), consumer);
		ds::MatcherFilter filter(matcher, prepender);
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			scan::scan(str::joinpath(m_dir, *i), filter);
	}
}

void Archive::queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype, const std::string& param)
{
	switch (qtype)
	{
		case BQ_DATA: {
			ds::DataOnly dataonly(out);
			queryMetadata(matcher, false, dataonly);
			break;
		}
		case BQ_POSTPROCESS: {
			Postprocess postproc(param, out);
			queryMetadata(matcher, false, postproc);

			// TODO: if we flush here, do we close the postprocessor for a further query?
			// TODO: POSTPROCESS isn't it just a query for the metadata?
			// TODO: in that case, the reader should implement it
			// TODO: as such: simpler code, and handle the case of
			// TODO: multiple archives nicely
			postproc.flush();
			break;
		}
		case BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(param);
			queryMetadata(matcher, false, rep);
			rep.report();
#endif
			break;
		}
		case BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(param);
			Summary s;
			querySummary(matcher, s);
			rep(s);
			rep.report();
#endif
			break;
		}
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)qtype));
	}
}

void Archive::querySummaries(const Matcher& matcher, Summary& summary) const
{
	vector<string> files;
	fileList(matcher, files);

	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
	{
		string pathname = str::joinpath(m_dir, *i);

		// Silently skip files that have been deleted
		if (!sys::fs::access(pathname + ".summary", R_OK))
			continue;

		Summary s;
		s.readFile(pathname + ".summary");
		s.filter(matcher, summary);
	}
}

void Archive::querySummary(const Matcher& matcher, Summary& summary)
{
	// Check if the matcher discriminates on reference times
	const matcher::Implementation* rtmatch = 0;
	if (matcher.m_impl)
		rtmatch = matcher.m_impl->get(types::TYPE_REFTIME);

	if (!rtmatch)
	{
		// The matcher does not contain reftime, we can work with a
		// global summary
		string cache_pathname = str::joinpath(m_dir, "summary");

		if (sys::fs::access(cache_pathname, R_OK))
		{
			Summary s;
			s.readFile(cache_pathname);
			s.filter(matcher, summary);
		} else if (sys::fs::access(m_dir, W_OK)) {
			// Rebuild the cache
			Summary s;
			querySummaries(Matcher(), s);

			// Save the summary
			s.writeAtomically(cache_pathname);

			// Query the newly generated summary that we still have
			// in memory
			s.filter(matcher, summary);
		} else
			querySummaries(matcher, summary);
	} else {
		querySummaries(matcher, summary);
	}
}

void Archive::acquire(const std::string& relname)
{
	// Scan file, reusing .metadata if still valid
	utils::metadata::Collector mdc;
	string pathname = str::joinpath(m_dir, relname);
	if (!scan::scan(pathname, mdc))
		throw wibble::exception::Consistency("acquiring " + pathname, "it does not look like a file we can acquire");
	acquire(relname, mdc);
}

void Archive::acquire(const std::string& relname, const utils::metadata::Collector& mds)
{
	string pathname = str::joinpath(m_dir, relname);
	time_t mtime = files::timestamp(pathname);
	if (mtime == 0)
		throw wibble::exception::Consistency("acquiring " + pathname, "file does not exist");

	// Compute the summary
	Summary sum;
	for (utils::metadata::Collector::const_iterator i = mds.begin();
			i != mds.end(); ++i)
		sum.add(*i);

	// Regenerate .metadata
	mds.writeAtomically(pathname + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(pathname + ".summary");

	// Add to index
	Item<types::Reftime> rt = sum.getReferenceTime();

	string bt;
	string et;

	switch (rt->style())
	{
		case types::Reftime::POSITION: {
			UItem<types::reftime::Position> p = rt.upcast<types::reftime::Position>();
			bt = et = p->time->toSQL();
			break;
		}
		case types::Reftime::PERIOD: {
			UItem<types::reftime::Period> p = rt.upcast<types::reftime::Period>();
			bt = p->begin->toSQL();
			et = p->end->toSQL();
		}
		default:
			throw wibble::exception::Consistency("unsupported reference time " + types::Reftime::formatStyle(rt->style()));
	}

	m_insert.reset();
	m_insert.bind(1, relname);
	m_insert.bind(2, mtime);
	m_insert.bind(3, bt);
	m_insert.bind(4, et);
	m_insert.step();
}

void Archive::remove(const std::string& relname)
{
	// Remove from index and from file system, including attached .metadata
	// and .summary, if they exist
}

void Archive::rescan(const std::string& relname)
{
	// TODO: generate metadata if outdated
	// TODO: generate summary if outdated
	// TODO: reindex if start and end are different than in the index
}

void Archive::maintenance(writer::MaintFileVisitor& v)
{
	time_t now = time(NULL);
	struct tm t;
	string delete_threshold;

	// Go to the beginning of the day
	now -= (now % (3600*24));

	// Compute the threshold for detecting files to delete
	if (m_delete_age != -1)
	{
		time_t del_thr = now - m_delete_age * 3600 * 24;
		gmtime_r(&del_thr, &t);
		delete_threshold = str::fmtf("%04d-%02d-%02d %02d:%02d:%02d",
				t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
				t.tm_hour, t.tm_min, t.tm_sec);
	}

	// List of files existing on disk
	writer::DirScanner disk(m_dir);

	Query q("sel_archive", m_db);
	q.compile("SELECT file, mtime, end_time FROM files ORDER BY file");

	while (q.step())
	{
		string file = q.fetchString(0);

		while (not disk.cur().empty() and disk.cur() < file)
		{
			v(disk.cur(), writer::MaintFileVisitor::ARC_TO_INDEX);
			disk.next();
		}
		if (disk.cur() == file)
		{
			disk.next();

			string pathname = str::joinpath(m_dir, file);

			time_t ts_data = files::timestamp(pathname);
			time_t ts_md = files::timestamp(pathname + ".metadata");
			time_t ts_sum = files::timestamp(pathname + ".summary");
			time_t ts_idx = q.fetchInt64(1);

			if (ts_idx != ts_data ||
			    ts_md < ts_data ||
			    ts_sum < ts_md)
				// Check timestamp consistency
				v(file, writer::MaintFileVisitor::ARC_TO_RESCAN);
			else if (q.fetchString(2) < delete_threshold)
				// Check for files older than delete age
				v(file, writer::MaintFileVisitor::ARC_TO_DELETE);
			else
				v(file, writer::MaintFileVisitor::ARC_OK);

			// TODO: if requested, check for internal consistency
			// TODO: it requires to have an infrastructure for quick
			// TODO:   consistency checkers (like, "GRIB starts with GRIB
			// TODO:   and ends with 7777")
		}
		else // if (disk.cur() > file)
			v(file, writer::MaintFileVisitor::ARC_DELETED);
	}
}

void Archive::repack(std::ostream& log, bool writable)
{
}

void Archive::check(std::ostream& log)
{
}

bool Archive::vacuum()
{
	// TODO: vacuum the database
	// TODO: regenerate master summary
	// Return true if it did anything (index shrunk or regenerated summary)
	return false;
}

}
}
}
