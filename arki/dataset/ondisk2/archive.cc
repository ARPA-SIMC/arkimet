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
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/utils/metadata.h>
#include <arki/scan/any.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils::sqlite;

namespace arki {
namespace dataset {
namespace ondisk2 {

Archive::Archive(const std::string& dir)
	: m_dir(dir), m_insert(m_db)
{
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
	m_insert.compile("INSERT INTO files (file, start_time, end_time) VALUES (?, ?, ?)");
}

void Archive::initDB()
{
	// Create the main table
	string query = "CREATE TABLE IF NOT EXISTS files ("
		"id INTEGER PRIMARY KEY,"
		" file TEXT NOT NULL,"
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
	if (const matcher::OR* reftime = matcher.m_impl->get(types::TYPE_REFTIME))
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
			query += " WHERE end_time > " + begin->toSQL();
		if (end.defined())
		{
			if (begin.defined())
				query += " AND start_time < " + end->toSQL();
			else
				query += " WHERE start_time < " + end->toSQL();
		}

		query += " ORDER BY file";
	} else {
		// No restrictions on reftime: get all files
		query = "SELECT file FROM files ORDER BY file";
	}

	
	Query q("sel_archive", m_db);
	q.compile(query);
	while (q.step())
		files.push_back(q.fetchString(0));
}

void Archive::queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer)
{
	vector<string> files;
	fileList(matcher, files);

#if 0 // TODO
	// TODO: does it make sense to check with the summary first?
	if (withData)
	{
		DataInliner inliner(consumer);
		PathPrepender prepender(sys::fs::abspath(m_root), inliner);
		MatcherFilter filter(matcher, prepender);
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			scan::scan(*i, filter);
	} else {
		PathPrepender prepender(sys::fs::abspath(m_root), consumer);
		MatcherFilter filter(matcher, prepender);
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			scan::scan(*i, filter);
	}
#endif
}

void Archive::queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype, const std::string& param)
{
#if 0 // TODO
	switch (qtype)
	{
		case BQ_DATA: {
			vector<string> files;
			fileList(matcher, files);

			DataOnly dataonly(out);
			PathPrepender prepender(sys::fs::abspath(m_root), dataonly);
			MatcherFilter filter(matcher, prepender);
			for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
				scan::scan(*i, filter);
			break;
		}
		case BQ_POSTPROCESS: {
			vector<string> files;
			fileList(matcher, files);

			Postprocess postproc(param, out);
			PathPrepender prepender(sys::fs::abspath(m_root), postproc);
			MatcherFilter filter(matcher, prepender);
			for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
				scan::scan(*i, filter);
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
#endif
}

void Archive::querySummary(const Matcher& matcher, Summary& summary)
{
	vector<string> files;
	fileList(matcher, files);

	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
	{
		// Silently skip files that have been deleted
		if (!sys::fs::access(*i + ".summary", R_OK))
			continue;

		Summary s;
		s.readFile(*i + ".summary");
		s.filter(matcher, summary);
	}
}

void Archive::acquire(const std::string& file)
{
	// Scan file, reusing .metadata if still valid
	utils::metadata::Collector mdc;
	if (!scan::scan(file, mdc))
		throw wibble::exception::Consistency("Cannot scan file " + file);

	// Compute the summary
	Summary sum;
	for (utils::metadata::Collector::const_iterator i = mdc.begin();
			i != mdc.end(); ++i)
		sum.add(*i);

	// Regenerate .metadata
	mdc.writeAtomically(file + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(file + ".summary");

	// Add to index
	Item<types::Reftime> rt = sum.getReferenceTime();

	string bt;
	string et;

	if (Item<types::reftime::Period> p = rt.upcast<types::reftime::Period>())
	{
		bt = p->begin->toSQL();
		et = p->end->toSQL();
	}
	else if (Item<types::reftime::Position> p = rt.upcast<types::reftime::Position>())
		bt = et = p->time->toSQL();
	else
		throw wibble::exception::Consistency("usupported reference time " + types::Reftime::formatStyle(rt->style()));

	m_insert.reset();
	m_insert.bind(1, file);
	m_insert.bind(2, bt);
	m_insert.bind(3, et);
	m_insert.step();
}

void Archive::remove(const std::string& file)
{
	// Remove from index and from file system, including attached .metadata
	// and .summary, if they exist
}


void Archive::maintenance(writer::MaintFileVisitor& v)
{
}

void Archive::repack(std::ostream& log, bool writable)
{
}

void Archive::check(std::ostream& log)
{
}

}
}
}
