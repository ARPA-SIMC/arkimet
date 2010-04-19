/*
 * dataset/ondisk2/archive - Handle archived data
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/index/base.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/utils/sqlite.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <fstream>
#include <ctime>
#include <cstdio>

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

namespace archive {

static bool matcher_extremes(const Matcher& matcher, UItem<types::Time>& begin, UItem<types::Time>& end)
{
	const matcher::OR* reftime = 0;

	begin.clear(); end.clear();
	
	if (!matcher.m_impl)
		return false;

	reftime = matcher.m_impl->get(types::TYPE_REFTIME);

	if (!reftime)
		return false;

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
	return begin.defined() || end.defined();
}

Manifest::~Manifest() {}

class PlainManifest : public Manifest
{
	struct Info
	{
		std::string file;
		time_t mtime;
		UItem<types::Time> start_time;
		UItem<types::Time> end_time;

		bool operator<(const Info& i) const
		{
			return file < i.file;
		}

		bool operator==(const Info& i) const
		{
			return file == i.file;
		}

		bool operator!=(const Info& i) const
		{
			return file != i.file;
		}

		void write(ostream& out) const
		{
			out << file << ";" << mtime << ";" << start_time->toSQL() << ";" << end_time->toSQL() << endl;
		}
	};
	std::string m_dir;
	vector<Info> info;
	ino_t last_inode;

	/**
	 * Reread the MANIFEST file.
	 *
	 * @returns true if the MANIFEST file existed, false if not
	 */
	bool reread()
	{
		string pathname(str::joinpath(m_dir, "MANIFEST"));
		ino_t inode = files::inode(pathname);

		if (inode == last_inode) return inode != 0;

		info.clear();
		last_inode = inode;
		if (last_inode == 0)
			return false;

		std::ifstream in;
		in.open(pathname.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(pathname, "opening file for reading");

		string line;
		for (size_t lineno = 1; !in.eof(); ++lineno)
		{
			Info item;

			getline(in, line);
			if (in.fail() && !in.eof())
				throw wibble::exception::File(pathname, "reading one line");

			// Skip empty lines
			if (line.empty()) continue;

			size_t beg = 0;
			size_t end = line.find(';');
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 1 field");

			item.file = line.substr(beg, end-beg);
			
			beg = end + 1;
			end = line.find(';', beg);
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 2 fields");

			item.mtime = strtoul(line.substr(beg, end-beg).c_str(), 0, 10);

			beg = end + 1;
			end = line.find(';', beg);
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 3 fields");

			item.start_time = Time::createFromSQL(line.substr(beg, end-beg));
			item.end_time = Time::createFromSQL(line.substr(end+1));

			info.push_back(item);
		}		

		in.close();
		return true;
	}

	void save()
	{
		string pathname(str::joinpath(m_dir, "MANIFEST.tmp"));

		std::ofstream out;
		out.open(pathname.c_str(), ios::out);
		if (!out.is_open() || out.fail())
			throw wibble::exception::File(pathname, "opening file for writing");

		for (vector<Info>::const_iterator i = info.begin();
				i != info.end(); ++i)
			i->write(out);

		out.close();

		if (::rename(pathname.c_str(), str::joinpath(m_dir, "MANIFEST").c_str()) < 0)
			throw wibble::exception::System("Renaming " + pathname + " to " + str::joinpath(m_dir, "MANIFEST"));
	}

public:
	PlainManifest(const std::string& dir)
		: m_dir(dir), last_inode(0)
	{
	}

	virtual ~PlainManifest()
	{
	}

	void openRO()
	{
		if (!reread())
			throw wibble::exception::Consistency("opening archive index", "MANIFEST does not exist in " + m_dir);
	}

	void openRW()
	{
		reread();
	}

	void fileList(const Matcher& matcher, std::vector<std::string>& files)
	{
		reread();

		string query;
		UItem<types::Time> begin;
		UItem<types::Time> end;
		if (matcher_extremes(matcher, begin, end))
		{
			// Get files with matching reftime
			for (vector<Info>::const_iterator i = info.begin();
					i != info.end(); ++i)
			{
				if (begin.defined() && i->end_time < begin) continue;
				if (end.defined() && i->start_time > end) continue;
				files.push_back(i->file);
			}
		} else {
			// No restrictions on reftime: get all files
			for (vector<Info>::const_iterator i = info.begin();
					i != info.end(); ++i)
				files.push_back(i->file);
		}
	}

	void vacuum()
	{
	}

	void acquire(const std::string& relname, time_t mtime, const Summary& sum)
	{
		reread();

		Info item;
		item.file = relname;
		item.mtime = mtime;

		// Add to index
		Item<types::Reftime> rt = sum.getReferenceTime();

		string bt;
		string et;

		switch (rt->style())
		{
			case types::Reftime::POSITION: {
				UItem<types::reftime::Position> p = rt.upcast<types::reftime::Position>();
				item.start_time = item.end_time = p->time;
				break;
		        }
			case types::Reftime::PERIOD: {
			        UItem<types::reftime::Period> p = rt.upcast<types::reftime::Period>();
				item.start_time = p->begin;
				item.end_time = p->end;
			        break;
		        }
			default:
			        throw wibble::exception::Consistency("unsupported reference time " + types::Reftime::formatStyle(rt->style()));
		}

		// Insertion sort; at the end, everything is already sorted and we
		// avoid inserting lots of duplicate items
		vector<Info>::iterator lb = lower_bound(info.begin(), info.end(), item);
		if (lb == info.end())
			info.push_back(item);
		else if (*lb != item)
			info.insert(lb, item);
		else
			*lb = item;

		save();
	}

	virtual void remove(const std::string& relname)
	{
		reread();

		vector<Info>::iterator i;
		for (i = info.begin(); i != info.end(); ++i)
			if (i->file == relname)
				break;
		if (i != info.end())
			info.erase(i);

		save();
	}

	virtual void check(writer::MaintFileVisitor& v)
	{
		reread();

		// List of files existing on disk
		writer::DirScanner disk(m_dir, true);

		for (vector<Info>::const_iterator i = info.begin(); i != info.end(); ++i)
		{
			while (not disk.cur().empty() and disk.cur() < i->file)
			{
				nag::verbose("Archive: %s is not in index", disk.cur().c_str());
				v(disk.cur(), writer::MaintFileVisitor::ARC_TO_INDEX);
				disk.next();
			}
			if (disk.cur() == i->file)
			{
				disk.next();

				string pathname = str::joinpath(m_dir, i->file);

				time_t ts_data = files::timestamp(pathname);
				if (ts_data == 0)
					ts_data = files::timestamp(pathname + ".gz");
				time_t ts_md = files::timestamp(pathname + ".metadata");
				time_t ts_sum = files::timestamp(pathname + ".summary");
				time_t ts_idx = i->mtime;

				if (ts_idx != ts_data ||
				    ts_md < ts_data ||
				    ts_sum < ts_md)
				{
					// Check timestamp consistency
					if (ts_idx != ts_data)
						nag::verbose("Archive: %s has a timestamp (%d) different than the one in the index (%d)",
								i->file.c_str(), ts_data, ts_idx);
					if (ts_md < ts_data)
						nag::verbose("Archive: %s has a timestamp (%d) newer that its metadata (%d)",
								i->file.c_str(), ts_data, ts_md);
					if (ts_md < ts_data)
						nag::verbose("Archive: %s metadata has a timestamp (%d) newer that its summary (%d)",
								i->file.c_str(), ts_md, ts_sum);
					v(i->file, writer::MaintFileVisitor::ARC_TO_RESCAN);
				}
				else
					v(i->file, writer::MaintFileVisitor::ARC_OK);

				// TODO: if requested, check for internal consistency
				// TODO: it requires to have an infrastructure for quick
				// TODO:   consistency checkers (like, "GRIB starts with GRIB
				// TODO:   and ends with 7777")
			}
			else // if (disk.cur() > i->file)
			{
				nag::verbose("Archive: %s has been deleted from the archive", i->file.c_str());
				v(i->file, writer::MaintFileVisitor::ARC_DELETED);
			}
		}
		while (not disk.cur().empty())
		{
			nag::verbose("Archive: %s is not in index", disk.cur().c_str());
			v(disk.cur(), writer::MaintFileVisitor::ARC_TO_INDEX);
			disk.next();
		}
	}
};


class SqliteManifest : public Manifest
{
	std::string m_dir;
	mutable utils::sqlite::SQLiteDB m_db;
	index::InsertQuery m_insert;

	void setupPragmas()
	{
		// Also, since the way we do inserts cause no trouble if a reader reads a
		// partial insert, we do not need read locking
		m_db.exec("PRAGMA read_uncommitted = 1");
		// Use new features, if we write we read it, so we do not need to
		// support sqlite < 3.3.0 if we are above that version
		m_db.exec("PRAGMA legacy_file_format = 0");
	}

	void initQueries()
	{
		m_insert.compile("INSERT INTO files (file, mtime, start_time, end_time) VALUES (?, ?, ?, ?)");
	}

	void initDB()
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


public:
	SqliteManifest(const std::string& dir)
		: m_dir(dir), m_insert(m_db)
	{
	}

	virtual ~SqliteManifest()
	{
	}

	void openRO()
	{
		string pathname(str::joinpath(m_dir, "index.sqlite"));
		if (m_db.isOpen())
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

		if (!wibble::sys::fs::access(pathname, F_OK))
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " does not exist");

		m_db.open(pathname);
		setupPragmas();

		initQueries();
	}

	void openRW()
	{
		string pathname(str::joinpath(m_dir, "index.sqlite"));
		if (m_db.isOpen())
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

		bool need_create = !wibble::sys::fs::access(pathname, F_OK);

		m_db.open(pathname);
		setupPragmas();
		
		if (need_create)
			initDB();

		initQueries();
	}

	void fileList(const Matcher& matcher, std::vector<std::string>& files)
	{
		string query;
		UItem<types::Time> begin;
		UItem<types::Time> end;
		if (matcher_extremes(matcher, begin, end))
		{
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

	void vacuum()
	{
		// Vacuum the database
		try {
			m_db.exec("VACUUM");
			m_db.exec("ANALYZE");
		} catch (std::exception& e) {
			nag::warning("ignoring failed attempt to optimize database: %s", e.what());
		}
	}

	void acquire(const std::string& relname, time_t mtime, const Summary& sum)
	{
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
			        break;
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

	virtual void remove(const std::string& relname)
	{
		Query q("del_file", m_db);
		q.compile("DELETE FROM files WHERE file=?");
		q.bind(1, relname);
		while (q.step())
			;
	}

	virtual void check(writer::MaintFileVisitor& v)
	{
		// List of files existing on disk
		writer::DirScanner disk(m_dir, true);

		Query q("sel_archive", m_db);
		q.compile("SELECT file, mtime FROM files ORDER BY file");

		while (q.step())
		{
			string file = q.fetchString(0);

			while (not disk.cur().empty() and disk.cur() < file)
			{
				nag::verbose("Archive: %s is not in index", disk.cur().c_str());
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
				time_t ts_idx = q.fetch<time_t>(1);

				if (ts_idx != ts_data ||
				    ts_md < ts_data ||
				    ts_sum < ts_md)
				{
					// Check timestamp consistency
					if (ts_idx != ts_data)
						nag::verbose("Archive: %s has a timestamp (%d) different than the one in the index (%d)",
								file.c_str(), ts_data, ts_idx);
					if (ts_md < ts_data)
						nag::verbose("Archive: %s has a timestamp (%d) newer that its metadata (%d)",
								file.c_str(), ts_data, ts_md);
					if (ts_md < ts_data)
						nag::verbose("Archive: %s metadata has a timestamp (%d) newer that its summary (%d)",
								file.c_str(), ts_md, ts_sum);
					v(file, writer::MaintFileVisitor::ARC_TO_RESCAN);
				}
				else
					v(file, writer::MaintFileVisitor::ARC_OK);

				// TODO: if requested, check for internal consistency
				// TODO: it requires to have an infrastructure for quick
				// TODO:   consistency checkers (like, "GRIB starts with GRIB
				// TODO:   and ends with 7777")
			}
			else // if (disk.cur() > file)
			{
				nag::verbose("Archive: %s has been deleted from the archive", disk.cur().c_str());
				v(file, writer::MaintFileVisitor::ARC_DELETED);
			}
		}
		while (not disk.cur().empty())
		{
			nag::verbose("Archive: %s is not in index", disk.cur().c_str());
			v(disk.cur(), writer::MaintFileVisitor::ARC_TO_INDEX);
			disk.next();
		}
	}

	static bool exists(const std::string& dir)
	{
		string pathname(str::joinpath(dir, "index.sqlite"));
		return wibble::sys::fs::access(pathname, F_OK);
	}
};

}

Archive::Archive(const std::string& dir)
	: m_dir(dir), m_mft(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);

	if (archive::SqliteManifest::exists(m_dir))
		m_mft = new archive::SqliteManifest(m_dir);
	else
		//m_mft = new archive::SqliteManifest(m_dir);
		m_mft = new archive::PlainManifest(m_dir);
}

Archive::~Archive()
{
	if (m_mft) delete m_mft;
}

void Archive::openRO()
{
	m_mft->openRO();
}

void Archive::openRW()
{
	m_mft->openRW();
}

void Archive::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	vector<string> files;
	m_mft->fileList(q.matcher, files);

	// TODO: does it make sense to check with the summary first?

	MetadataConsumer* c = &consumer;
	auto_ptr<sort::Stream> sorter;
	auto_ptr<ds::DataInliner> inliner;

	if (q.withData)
	{
		inliner.reset(new ds::DataInliner(*c));
		c = inliner.get();
	}
		
	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

	ds::MatcherFilter filter(q.matcher, *c);
	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
		scan::scan(str::joinpath(m_dir, *i), filter);
}

void Archive::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA: {
			ds::DataOnly dataonly(out);
			queryData(q, dataonly);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out);
			queryData(q, postproc);

			// TODO: if we flush here, do we close the postprocessor for a further query?
			// TODO: POSTPROCESS isn't it just a query for the metadata?
			// TODO: in that case, the reader should implement it
			// TODO: as such: simpler code, and handle the case of
			// TODO: multiple archives nicely
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			queryData(q, rep);
			rep.report();
#endif
			break;
		}
		case dataset::ByteQuery::BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			Summary s;
			querySummary(q.matcher, s);
			rep(s);
			rep.report();
#endif
			break;
		}
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)q.type));
	}
}

void Archive::querySummaries(const Matcher& matcher, Summary& summary)
{
	vector<string> files;
	m_mft->fileList(matcher, files);

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

	// Iterate the metadata, computing the summary and making the data
	// paths relative
	Summary sum;
	for (utils::metadata::Collector::const_iterator i = mds.begin();
			i != mds.end(); ++i)
	{
		Item<source::Blob> s = i->source.upcast<source::Blob>();
		s->filename = str::basename(s->filename);
		sum.add(*i);
	}

	// Regenerate .metadata
	mds.writeAtomically(pathname + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(pathname + ".summary");

	// Add to manifest
	m_mft->acquire(relname, mtime, sum);
}

void Archive::maintenance(writer::MaintFileVisitor& v)
{
	m_mft->check(v);
}

/*
void Archive::repack(std::ostream& log, bool writable)
{
}

void Archive::check(std::ostream& log)
{
}
*/

void Archive::remove(const std::string& relname)
{
	// Remove from index and from file system, including attached .metadata
	// and .summary, if they exist
	string pathname = str::joinpath(m_dir, relname);

	sys::fs::deleteIfExists(pathname + ".summary");
	sys::fs::deleteIfExists(pathname + ".metadata");
	sys::fs::deleteIfExists(pathname);

	deindex(relname);
}

void Archive::deindex(const std::string& relname)
{
	m_mft->remove(relname);
}

void Archive::rescan(const std::string& relname)
{
	string pathname = str::joinpath(m_dir, relname);
	time_t ts_data = files::timestamp(pathname);
	time_t ts_md = files::timestamp(pathname + ".metadata");

	// Invalidate summary
	sys::fs::deleteIfExists(pathname + ".summary");

	// Invalidate metadata if older than data
	if (ts_md < ts_data)
		sys::fs::deleteIfExists(pathname + ".metadata");

	// Deindex the file
	deindex(relname);

	// Reindex the file
	acquire(relname);
}

void Archive::vacuum()
{
	// If archive dir is not writable, skip this section
	if (!sys::fs::access(m_dir, W_OK)) return;

	m_mft->vacuum();

	// Regenerate summary cache
	Summary s;
	querySummaries(Matcher(), s);
	s.writeAtomically(str::joinpath(m_dir, "summary"));
}

Archives::Archives(const std::string& dir, bool read_only)
	: m_dir(dir), m_read_only(read_only), m_last(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);

	// Look for subdirectories: they are archives
	sys::fs::Directory d(m_dir);
	for (sys::fs::Directory::const_iterator i = d.begin();
			i != d.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.')
			continue;
		string pathname = str::joinpath(m_dir, *i);
		if (!sys::fs::isDirectory(pathname))
			continue;
		Archive* a = 0;
		if (*i == "last")
			m_last = a = new Archive(pathname);
		else
			m_archives.insert(make_pair(*i, a = new Archive(pathname)));

		if (a)
		{
			if (read_only)
				a->openRO();
			else
				a->openRW();
		}
	}

	// Instantiate the 'last' archive even if the directory does not exist,
	// if not read only
	if (!read_only && !m_last)
	{
		m_last = new Archive(str::joinpath(m_dir, "last"));
		m_last->openRW();
	}
}

Archives::~Archives()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		delete i->second;
	if (m_last)
		delete m_last;
}

Archive* Archives::lookup(const std::string& name)
{
	if (name == "last")
		return m_last;

	std::map<std::string, Archive*>::iterator i = m_archives.find(name);
	if (i == m_archives.end())
		return 0;
	return i->second;
}

void Archives::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->queryData(q, consumer);
	if (m_last)
		m_last->queryData(q, consumer);
}

void Archives::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->queryBytes(q, out);
	if (m_last)
		m_last->queryBytes(q, out);
}

void Archives::querySummary(const Matcher& matcher, Summary& summary)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->querySummary(matcher, summary);
	if (m_last)
		m_last->querySummary(matcher, summary);
}

static std::string poppath(std::string& path)
{
	size_t start = 0;
	while (start < path.size() && path[start] == '/')
		++start;
	size_t end = start;
	while (end < path.size() && path[end] != '/')
		++end;
	size_t nstart = end;
	while (nstart < path.size() && path[nstart] == '/')
		++nstart;
	string res = path.substr(start, end-start);
	path = path.substr(nstart);
	return res;
}

void Archives::acquire(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->acquire(path);
	else
		throw wibble::exception::Consistency("acquiring " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::acquire(const std::string& relname, const utils::metadata::Collector& mds)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->acquire(path, mds);
	else
		throw wibble::exception::Consistency("acquiring " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::remove(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->remove(path);
	else
		throw wibble::exception::Consistency("removing " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::rescan(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->rescan(path);
	else
		throw wibble::exception::Consistency("rescanning " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

namespace writer {
struct MaintPathPrepender : public MaintFileVisitor
{
	MaintFileVisitor& next;
	std::string prefix;

	MaintPathPrepender(MaintFileVisitor& next, const std::string& prefix)
		: next(next), prefix(prefix) {}
	
	virtual void operator()(const std::string& file, State state)
	{
		next(str::joinpath(prefix, file), state);
	}
};
}

void Archives::maintenance(writer::MaintFileVisitor& v)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
	{
		writer::MaintPathPrepender p(v, i->first);
		i->second->maintenance(p);
	}
	if (m_last)
	{
		writer::MaintPathPrepender p(v, "last");
		m_last->maintenance(p);
	}
}

void Archives::vacuum()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->vacuum();
	if (m_last)
		m_last->vacuum();
}

}
}
}
