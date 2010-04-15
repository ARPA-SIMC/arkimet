/*
 * dataset/index - Dataset index infrastructure
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

#include <arki/dataset/ondisk2/index.h>
#include <arki/dataset/ondisk2/maintenance.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/dataset.h>
#include <arki/types/reftime.h>
#include <arki/types/source.h>
#include <arki/types/assigneddataset.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/sort.h>
#include <arki/nag.h>
#include <arki/runtime/io.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>
#include <wibble/grcal/grcal.h>

#include <sstream>
#include <ctime>
#include <cassert>
#include <cerrno>
#include <cstdlib>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;
using namespace arki::dataset::index;

namespace arki {
namespace dataset {
namespace ondisk2 {

struct IndexGlobalData
{
	std::set<types::Code> all_components;

	IndexGlobalData() {
		all_components.insert(types::TYPE_ORIGIN);
		all_components.insert(types::TYPE_PRODUCT);
		all_components.insert(types::TYPE_LEVEL);
		all_components.insert(types::TYPE_TIMERANGE);
		all_components.insert(types::TYPE_AREA);
		all_components.insert(types::TYPE_ENSEMBLE);
		all_components.insert(types::TYPE_RUN);
		//all_components.insert(types::TYPE_REFTIME);
	}
};
static IndexGlobalData igd;

Index::Index(const ConfigFile& cfg)
	: m_name(cfg.value("name")), m_root(cfg.value("path")),
	  m_get_id("getid", m_db),
	  m_uniques(0), m_others(0)
{
	m_indexpath = cfg.value("indexfile");
	if (m_indexpath.empty())
		m_indexpath = "index.sqlite";

	m_scache_root = str::joinpath(m_root, ".summaries");

	m_pathname = m_root.empty() ? m_indexpath : str::joinpath(m_root, m_indexpath);

	// What metadata components we index
	string index = cfg.value("index");
	if (index.empty())
		index = "origin, product, level, timerange, area, ensemble, run";
	m_components_indexed = parseMetadataBitmask(index);

	// What metadata components we use to create a unique id
	std::set<types::Code> unique_members = parseMetadataBitmask(cfg.value("unique"));
	unique_members.erase(types::TYPE_REFTIME);
	if (not unique_members.empty())
		m_uniques = new Aggregate(m_db, "mduniq", unique_members);

	// What metadata components do not count for uniqueness
	std::set<types::Code> other_members;
	for (std::set<types::Code>::const_iterator i = igd.all_components.begin();
		i != igd.all_components.end(); ++i)
		if (unique_members.find(*i) == unique_members.end())
			other_members.insert(*i);
	if (not other_members.empty())
		m_others = new Aggregate(m_db, "mdother", other_members);
}

Index::~Index()
{
	if (m_uniques) delete m_uniques;
	if (m_others) delete m_others;
}

void Index::initQueries()
{
	if (m_uniques) m_uniques->initQueries();
	if (m_others) m_others->initQueries();

	string query = "SELECT id FROM md WHERE reftime=?";
	if (m_uniques) query += " AND uniq=?";
	if (m_others) query += " AND other=?";
	m_get_id.compile(query);
}

std::set<types::Code> Index::unique_codes() const
{
	std::set<types::Code> res;
	if (m_uniques) res = m_uniques->members();
	res.insert(types::TYPE_REFTIME);
	return res;
}

void Index::setupPragmas()
{
	// Faster but riskier, since we do not have a flagfile to trap
	// interrupted transactions
	//m_db.exec("PRAGMA synchronous = OFF");
	// Faster but riskier, since we do not have a flagfile to trap
	// interrupted transactions
	//m_db.exec("PRAGMA journal_mode = MEMORY");
	// Truncate the journal instead of delete: faster on many file systems
	// m_db.exec("PRAGMA journal_mode = TRUNCATE");
	// Zero the header of the journal instead of delete: faster on many file systems
	m_db.exec("PRAGMA journal_mode = PERSIST");
	// Also, since the way we do inserts cause no trouble if a reader reads a
	// partial insert, we do not need read locking
	m_db.exec("PRAGMA read_uncommitted = 1");
	// Use new features, if we write we read it, so we do not need to
	// support sqlite < 3.3.0 if we are above that version
	m_db.exec("PRAGMA legacy_file_format = 0");
}

int Index::id(const Metadata& md) const
{
	m_get_id.reset();

	int idx = 0;
	UItem<> urt = md.get(types::TYPE_REFTIME);
	if (!urt.defined()) return -1;
	Item<types::Reftime> rt(urt.upcast<types::Reftime>());
	if (rt->style() != types::Reftime::POSITION) return -1;
	m_get_id.bind(++idx, rt.upcast<types::reftime::Position>()->time->toSQL());

	if (m_uniques)
	{
		int id = m_uniques->get(md);
		// If we do not have this aggregate, then we do not have this metadata
		if (id == -1) return -1;
		m_get_id.bind(++idx, id);
	}

	if (m_others)
	{
		int id = m_others->get(md);
		// If we do not have this aggregate, then we do not have this metadata
		if (id == -1) return -1;
		m_get_id.bind(++idx, id);
	}

	int id = -1;
	while (m_get_id.step())
		id = m_get_id.fetch<int>(0);

	return id;
}

size_t Index::count() const
{
	Query sq("count", m_db);
	sq.compile("SELECT COUNT(*) FROM md");
	size_t res = 0;
	while (sq.step())
		res = sq.fetch<size_t>(0);
	return res;
}

// IndexFileVisitor that takes a sequence ordered by file and produces a
// sequence ordered by file, reftime, offset
namespace {
struct FurtherSort
{
	struct FileData
	{
		int id;
		std::string reftime;
		off_t offset;
		size_t size;

		FileData(int id, const std::string& reftime, off_t offset, size_t size)
			: id(id), reftime(reftime), offset(offset), size(size) {}
	};

	static bool sorter(const FileData& a, const FileData& b)
	{
		if (a.reftime != b.reftime)
			return a.reftime < b.reftime;
		return a.offset < b.offset;
	}

	writer::IndexFileVisitor& next;
	string curfile;
	vector<FileData> data;

	FurtherSort(writer::IndexFileVisitor& next) : next(next) {}

	virtual void operator()(const std::string& file, const std::string& reftime, int id, off_t offset, size_t size)
	{
		if (file != curfile)
		{
			emit();
			curfile = file;
		}
		data.push_back(FileData(id, reftime, offset, size));
	}

	void end()
	{
		emit();
	}

	void emit()
	{
		if (data.empty())
			return;
		std::sort(data.begin(), data.end(), sorter);
		for (vector<FileData>::const_iterator i = data.begin(); i != data.end(); ++i)
			next(curfile, i->id, i->offset, i->size);
		data.clear();
	}
};
}

void Index::scan_files(writer::IndexFileVisitor& v) const
{
	Query sq("scan_files", m_db);
	sq.compile("SELECT id, file, reftime, offset, size FROM md ORDER BY file");
	FurtherSort fs(v);
	while (sq.step())
	{
		int id = sq.fetch<int>(0);
		string file = sq.fetchString(1);
		string reftime = sq.fetchString(2);
		off_t offset = sq.fetch<off_t>(3);
		size_t size = sq.fetch<size_t>(4);

		fs(file, reftime, id, offset, size);
	}
	fs.end();
}

void Index::scan_file(const std::string& relname, writer::IndexFileVisitor& v, const std::string& orderBy) const
{
	Query sq("scan_file", m_db);
	sq.compile("SELECT id, offset, size FROM md WHERE file=? ORDER BY " + orderBy);
	sq.bind(1, relname);
	while (sq.step())
	{
		int id = sq.fetch<int>(0);
		off_t offset = sq.fetch<off_t>(1);
		size_t size = sq.fetch<size_t>(2);
		v(relname, id, offset, size);
	}
}

void Index::scan_file(const std::string& relname, MetadataConsumer& consumer) const
{
	string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";
	if (m_uniques) query += ", m.uniq";
	if (m_others) query += ", m.other";
	query += " FROM md AS m";
	/*
	if (m_uniques)
		query += " JOIN mduniq AS u ON uniq = u.id";
	if (m_others)
		query += " JOIN mdother AS o ON other = o.id";
	*/
	query += " WHERE m.file=? ORDER BY m.offset";

	Query mdq("scan_file_md", m_db);
	mdq.compile(query);
	mdq.bind(1, relname);

	while (mdq.step())
	{
		// Rebuild the Metadata
		Metadata md;
		md.create();
		md.set(types::AssignedDataset::create(m_name, str::fmt(mdq.fetch<int>(0))));
		md.source = source::Blob::create(
				mdq.fetchString(1), mdq.fetchString(2),
				mdq.fetch<size_t>(3), mdq.fetch<size_t>(4));
		//md.notes = mdq.fetchItems<types::Note>(5);
		const void* notes_p = mdq.fetchBlob(5);
		int notes_l = mdq.fetchBytes(5);
		md.set_notes_encoded(string((const char*)notes_p, notes_l));
		md.set(reftime::Position::create(Time::createFromSQL(mdq.fetchString(6))));
		int j = 7;
		if (m_uniques)
		{
			if (mdq.fetchType(j) != SQLITE_NULL)
				m_uniques->read(mdq.fetch<int>(j), md);
			++j;
		}
		if (m_others)
		{
			if (mdq.fetchType(j) != SQLITE_NULL)
				m_others->read(mdq.fetch<int>(j), md);
			++j;
		}

		consumer(md);
	}
}

std::string Index::max_file_reftime(const std::string& relname) const
{
	Query sq("max_file_reftime", m_db);
	sq.compile("SELECT MAX(reftime) FROM md WHERE file=?");
	sq.bind(1, relname);
	string res;
	while (sq.step())
		res = sq.fetchString(0);
	return res;
}

// Compute the maximum datetime span of the reftime query
static void datetime_span(const matcher::OR* reftime, UItem<types::Time>& begin, UItem<types::Time>& end)
{
	begin.clear();
	end.clear();

	// Start with the interval of the first matcher
	matcher::OR::const_iterator i = reftime->begin();
	(*i)->upcast<matcher::MatchReftime>()->dateRange(begin, end);

	// Enlarge with the interval of the following matchers, since they are
	// all ORed together
	for (++i; i != reftime->end(); ++i)
	{
		UItem<types::Time> b, e;
		(*i)->upcast<matcher::MatchReftime>()->dateRange(b, e);
		if (!b.defined() || b < begin)
			begin = b;
		if (!e.defined() || (end.defined() && e > end))
			end = e;
	}
}

static void db_time_extremes(utils::sqlite::SQLiteDB& db, UItem<types::Time>& begin, UItem<types::Time>& end)
{
	// SQLite can compute min and max of an indexed column very fast,
	// provided that it is the ONLY thing queried.
	Query q1("min_date", db);
	q1.compile("SELECT MIN(reftime) FROM md");
	while (q1.step())
	{
		if (q1.fetchType(0) != SQLITE_NULL)
			begin = types::Time::createFromSQL(q1.fetchString(0));
	}

	Query q2("min_date", db);
	q2.compile("SELECT MAX(reftime) FROM md");
	while (q2.step())
	{
		if (q2.fetchType(0) != SQLITE_NULL)
			end = types::Time::createFromSQL(q2.fetchString(0));
	}
}

bool Index::addJoinsAndConstraints(const Matcher& m, std::string& query) const
{
	vector<string> constraints;

	// Add database constraints
	if (not m.empty())
	{
		if (const matcher::OR* reftime = m.m_impl->get(types::TYPE_REFTIME))
		{
			UItem<types::Time> begin, end;
			datetime_span(reftime, begin, end);
			if (begin.defined() || end.defined())
			{
				// Compare with the reftime bounds in the database
				UItem<types::Time> db_begin, db_end;
				db_time_extremes(m_db, db_begin, db_end);
				if (db_begin.defined() && db_end.defined())
				{
					// If the bounds of the query time are
					// open, or go past the bounds of the
					// database, replace them with the
					// database bounds
					if (!begin.defined() || begin < db_begin) begin = db_begin;
					if (!end.defined() || end > db_end) end = db_end;
					long long int qrange = grcal::date::duration(begin->vals, end->vals);
					long long int dbrange = grcal::date::duration(db_begin->vals, db_end->vals);
					// If the query chooses less than 20%
					// if the time span, force the use of
					// the reftime index
					if (dbrange > 0 && qrange * 100 / dbrange < 20)
					{
						query += " INDEXED BY md_idx_reftime";
						constraints.push_back("reftime BETWEEN \'" + begin->toSQL()
							    + "\' AND \'" + end->toSQL() + "\'");
					}
				}
			}

			string constraint = "(";
			for (matcher::OR::const_iterator j = reftime->begin(); j != reftime->end(); ++j)
			{
				if (j != reftime->begin())
					constraint += " OR ";
				constraint += (*j)->upcast<matcher::MatchReftime>()->sql("reftime");
			}
			constraint += ")";
			constraints.push_back(constraint);
		}

		// Join with the aggregate tables and add constraints on aggregate tables
		if (m_uniques)
		{
			string s = m_uniques->make_subquery(m);
			if (!s.empty())
				constraints.push_back("uniq IN ("+s+")");
		}

		if (m_others)
		{
			string s = m_others->make_subquery(m);
			if (!s.empty())
				constraints.push_back("other IN ("+s+")");
		}
	}

	/*
	if (not m.empty() and constraints.size() != m.m_impl->size())
		// We can only see what we index, the rest is lost
		// TODO: add a filter to the query results, before the
		//       postprocessor that pulls in data from disk
		return false;
	*/

	if (!constraints.empty())
		query += " WHERE " + str::join(constraints.begin(), constraints.end(), " AND ");
	return true;
}

bool Index::query(const dataset::DataQuery& q, MetadataConsumer& consumer) const
{
	string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";

	if (m_uniques) query += ", m.uniq";
	if (m_others) query += ", m.other";

	query += " FROM md AS m";

	try {
		if (!addJoinsAndConstraints(q.matcher, query))
			return false;
	} catch (NotFound) {
		// If one of the subqueries did not find any match, we can directly
		// return true here, as we are not going to get any result
		return true;
	}

	query += " ORDER BY m.reftime";

	nag::verbose("Running query %s", query.c_str());

	metadata::Collector mdbuf;
	string last_fname;
	auto_ptr<runtime::Tempfile> tmpfile;

	// Limited scope for mdq, so we finalize the query before starting to
	// emit results
	{
		Query mdq("mdq", m_db);
		mdq.compile(query);

		// TODO: see if it's worth sorting mdbuf by file and offset

//fprintf(stderr, "PRE\n");
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
		while (mdq.step())
		{
			// At file boundary, sort and write out what we have so
			// far, so we don't keep it all in memory
			string srcname = mdq.fetchString(2);
			if (srcname != last_fname)
			{
				if (mdbuf.size() > 8192)
				{
					// If we pile up too many metadata, write them out
					if (q.sorter)
						std::sort(mdbuf.begin(), mdbuf.end(), sort::STLCompare(*q.sorter));
					if (tmpfile.get() == 0)
						tmpfile.reset(new runtime::Tempfile);
					mdbuf.writeTo(tmpfile->stream(), tmpfile->name());
					tmpfile->stream().flush();
					mdbuf.clear();
				}
				last_fname = srcname;
			}

			// Rebuild the Metadata
			Metadata md;
			md.create();
			md.set(types::AssignedDataset::create(m_name, str::fmt(mdq.fetch<int>(0))));
			md.source = source::Blob::create(
					mdq.fetchString(1), mdq.fetchString(2),
					mdq.fetch<size_t>(3), mdq.fetch<size_t>(4));
			// md.notes = mdq.fetchItems<types::Note>(5);
			const void* notes_p = mdq.fetchBlob(5);
			int notes_l = mdq.fetchBytes(5);
			md.set_notes_encoded(string((const char*)notes_p, notes_l));
			md.set(reftime::Position::create(Time::createFromSQL(mdq.fetchString(6))));
			int j = 7;
			if (m_uniques)
			{
				if (mdq.fetchType(j) != SQLITE_NULL)
					m_uniques->read(mdq.fetch<int>(j), md);
				++j;
			}
			if (m_others)
			{
				if (mdq.fetchType(j) != SQLITE_NULL)
					m_others->read(mdq.fetch<int>(j), md);
				++j;
			}

			// Buffer the results in memory, to release the database lock as soon as possible
			mdbuf(md);
		}
//fprintf(stderr, "POST %zd\n", mdbuf.size());
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
	}
//if (tmpfile.get()) system(str::fmtf("ls -la --si %s >&2", tmpfile->name().c_str()).c_str());

	// Replay the data from the temporary file
	if (tmpfile.get() != 0)
		Metadata::readFile(tmpfile->name(), consumer);

	// Sort and output the rest
	if (q.sorter)
		std::sort(mdbuf.begin(), mdbuf.end(), sort::STLCompare(*q.sorter));

	// pass it to consumer
	mdbuf.sendTo(consumer);

//fprintf(stderr, "POSTQ %zd\n", mdbuf.size());
//system(str::fmtf("ps u %d >&2", getpid()).c_str());

	return true;
}

void Index::invalidateSummaryCache(int year, int month)
{
	if (sys::fs::deleteIfExists(str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", year, month))))
		sys::fs::deleteIfExists(str::joinpath(m_scache_root, "all.summary"));
}

void Index::invalidateSummaryCache(const Metadata& md)
{
	Item<types::reftime::Position> rt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>();
	invalidateSummaryCache(rt->time->vals[0], rt->time->vals[1]);
}

void Index::rebuildSummaryCache()
{
	// Delete all files *.summary in the cache directory
	sys::fs::Directory dir(m_scache_root);
	for (sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
		if (str::endsWith(*i, ".summary"))
		{
			string pathname = str::joinpath(m_scache_root, *i);
			if (unlink(pathname.c_str()) < 0)
				throw wibble::exception::System("deleting file " + pathname);
		}
	// Rebuild all summaries
	summaryForAll();
}

void Index::querySummaryFromDB(const std::string& where, Summary& summary) const
{
	string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

	if (m_uniques) query += ", uniq";
	if (m_others) query += ", other";

	query += " FROM md";
	if (!where.empty())
		query += " WHERE " + where;

	if (m_uniques && m_others)
		query += " GROUP BY uniq, other";
	else if (m_uniques)
		query += " GROUP BY uniq";
	else if (m_others)
		query += " GROUP BY other";

	nag::verbose("Running query %s", query.c_str());

	Query sq("sq", m_db);
	sq.compile(query);

	while (sq.step())
	{
		// Fill in the summary statistics
		arki::Item<summary::Stats> st(new summary::Stats);
		st->count = sq.fetch<size_t>(0);
		st->size = sq.fetch<unsigned long long>(1);
		Item<Time> min_time = Time::createFromSQL(sq.fetchString(2));
		Item<Time> max_time = Time::createFromSQL(sq.fetchString(3));
		st->reftimeMerger.mergeTime(min_time, max_time);

		// Fill in the metadata fields
		Metadata md;
		md.create();
		int j = 4;
		if (m_uniques)
		{
			if (sq.fetchType(j) != SQLITE_NULL)
				m_uniques->read(sq.fetch<int>(j), md);
			++j;
		}
		if (m_others)
		{
			if (sq.fetchType(j) != SQLITE_NULL)
				m_others->read(sq.fetch<int>(j), md);
			++j;
		}

		// Feed the results to the summary
		summary.add(md, st);
	}
}

Summary Index::summaryForMonth(int year, int month) const
{
	Summary s;
	string sum_file = str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", year, month));
	bool has_cache = true;
	int fd = open(sum_file.c_str(), O_RDONLY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			has_cache = false;
		else
			throw wibble::exception::System("opening file " + sum_file);
	}
	utils::HandleWatch hw(sum_file, fd);

	if (has_cache)
		s.read(fd, sum_file);
	else
	{
		int nextyear = year + (month/12);
		int nextmonth = (month % 12) + 1;

		querySummaryFromDB(
			 "reftime >= " + str::fmtf("'%04d-%02d-01 00:00:00'", year, month)
		       + " AND reftime < " + str::fmtf("'%04d-%02d-01 00:00:00'", nextyear, nextmonth), s);

		// Write back to the cache directory, if allowed
		if (sys::fs::access(m_scache_root, W_OK))
			s.writeAtomically(sum_file);
	}

	return s;
}

Summary Index::summaryForAll() const
{
	Summary s;
	string sum_file = str::joinpath(m_scache_root, "all.summary");
	bool has_cache = true;
	int fd = open(sum_file.c_str(), O_RDONLY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			has_cache = false;
		else
			throw wibble::exception::System("opening file " + sum_file);
	}
	utils::HandleWatch hw(sum_file, fd);

	if (has_cache)
		s.read(fd, sum_file);
	else
	{
		// Find the datetime extremes in the database
		UItem<types::Time> begin, end;
		db_time_extremes(m_db, begin, end);

		// If there is data in the database, get all the involved
		// monthly summaries
		if (begin.defined() && end.defined())
		{
			int year = begin->vals[0];
			int month = begin->vals[1];
			while (year < end->vals[0] || (year == end->vals[0] && month <= end->vals[1]))
			{
				s.add(summaryForMonth(year, month));

				// Increment the month
				month = (month%12) + 1;
				if (month == 1)
					++year;
			}
		}

		// Write back to the cache directory, if allowed
		if (sys::fs::access(m_scache_root, W_OK))
			s.writeAtomically(sum_file);
	}

	return s;
}

bool Index::querySummaryFromDB(const Matcher& m, Summary& summary) const
{
	string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

	if (m_uniques) query += ", uniq";
	if (m_others) query += ", other";

	query += " FROM md";

	try {
		if (!addJoinsAndConstraints(m, query))
			return false;
	} catch (NotFound) {
		// If one of the subqueries did not find any match, we can directly
		// return true here, as we are not going to get any result
		return true;
	}

	if (m_uniques && m_others)
		query += " GROUP BY uniq, other";
	else if (m_uniques)
		query += " GROUP BY uniq";
	else if (m_others)
		query += " GROUP BY other";

	nag::verbose("Running query %s", query.c_str());

	Query sq("sq", m_db);
	sq.compile(query);

	while (sq.step())
	{
		// Fill in the summary statistics
		arki::Item<summary::Stats> st(new summary::Stats);
		st->count = sq.fetch<size_t>(0);
		st->size = sq.fetch<unsigned long long>(1);
		Item<Time> min_time = Time::createFromSQL(sq.fetchString(2));
		Item<Time> max_time = Time::createFromSQL(sq.fetchString(3));
		st->reftimeMerger.mergeTime(min_time, max_time);

		// Fill in the metadata fields
		Metadata md;
		md.create();
		int j = 4;
		if (m_uniques)
		{
			if (sq.fetchType(j) != SQLITE_NULL)
				m_uniques->read(sq.fetch<int>(j), md);
			++j;
		}
		if (m_others)
		{
			if (sq.fetchType(j) != SQLITE_NULL)
				m_others->read(sq.fetch<int>(j), md);
			++j;
		}

		// Feed the results to the summary
		summary.add(md, st);
	}

	return true;
}

static inline void end_this_month(Item<types::Time>& t)
{
	// Advance to the end of this month
	t->vals[2] = grcal::date::daysinmonth(t->vals[0], t->vals[1]);
	t->vals[3] = 23;
	t->vals[4] = 59;
	t->vals[5] = 59;
}
static inline bool range_evelopes_full_month(const Item<types::Time>& begin, const Item<types::Time>& end)
{
	bool begins_at_beginning = begin->vals[2] == 1 &&
		begin->vals[3] == 0 && begin->vals[4] == 0 && begin->vals[5] == 0;
	if (begins_at_beginning)
	{
		Item<types::Time> t = types::Time::create(begin->vals);
		end_this_month(t);
		return end >= t;
	}

	bool ends_at_end = end->vals[2] == grcal::date::daysinmonth(end->vals[0], end->vals[1]) &&
		end->vals[3] == 23 && end->vals[4] == 59 && end->vals[5] == 59;
	if (ends_at_end)
	{
		Item<types::Time> t = types::Time::create(end->vals);
		t->vals[2] = 1;
		t->vals[3] = t->vals[4] = t->vals[5] = 0;
		return begin <= t;
	}

	return end->vals[0] == begin->vals[0] + begin->vals[1]/12 &&
	       end->vals[1] == (begin->vals[1] % 12) + 1;
}

bool Index::querySummary(const Matcher& matcher, Summary& summary) const
{
	// Check if the matcher discriminates on reference times
	const matcher::OR* reftime = 0;
	if (matcher.m_impl)
		reftime = matcher.m_impl->get(types::TYPE_REFTIME);

	if (!reftime)
	{
		// The matcher does not contain reftime, we can work with a
		// global summary
		Summary s = summaryForAll();
		s.filter(matcher, summary);
		return true;
	}

	// Get the bounds of the query
	UItem<types::Time> begin, end;
	datetime_span(reftime, begin, end);

	// If the reftime query queries all the time span, use the global index
	if (!begin.defined() && !end.defined())
	{
		Summary s = summaryForAll();
		s.filter(matcher, summary);
		return true;
	}

	// Amend open ends with the bounds from the database
	UItem<types::Time> db_begin, db_end;
	db_time_extremes(m_db, db_begin, db_end);
	// If the database is empty then the result is empty:
	// we are done
	if (!db_begin.defined())
		return true;
	bool begin_from_db = false;
	if (!begin.defined() || begin < db_begin)
	{
		begin = db_begin;
		begin_from_db = true;
	}
	bool end_from_db = false;
	if (!end.defined() || end > db_end)
	{
		end = db_end;
		end_from_db = true;
	}

	// If the interval is under a week, query the DB directly
	long long int range = grcal::date::duration(begin->vals, end->vals);
	if (range <= 7 * 24 * 3600)
		return querySummaryFromDB(matcher, summary);

	if (begin_from_db)
	{
		// Round down to month begin, so we reuse the cached summary if
		// available
		begin->vals[2] = 1;
		begin->vals[3] = 0;
		begin->vals[4] = 0;
		begin->vals[5] = 0;
	}
	if (end_from_db)
	{
		// Round up to month end, so we reuse the cached summary if
		// available
		end->vals[2] = grcal::date::daysinmonth(end->vals[0], end->vals[1]);
		end->vals[3] = 23;
		end->vals[4] = 59;
		end->vals[5] = 59;
	}

	// If the selected interval does not envelope any whole month, query
	// the DB directly
	if (!range_evelopes_full_month(begin, end))
		return querySummaryFromDB(matcher, summary);

	// Query partial month at beginning, middle whole months, partial
	// month at end. Query whole months at extremes if they are indeed whole
	while (begin <= end)
	{
		Item<types::Time> endmonth = types::Time::create(begin->vals);
		end_this_month(endmonth);

		bool starts_at_beginning = (begin->vals[2] == 1 &&
				begin->vals[3] == 0 && begin->vals[4] == 0 && begin->vals[5] == 0);
		if (starts_at_beginning && endmonth <= end)
		{
			Summary s = summaryForMonth(begin->vals[0], begin->vals[1]);
			s.filter(matcher, summary);
		} else if (endmonth <= end) {
			Summary s;
			querySummaryFromDB("reftime >= '" + begin->toSQL() + "'"
					   " AND reftime < '" + endmonth->toSQL() + "'", s);
			s.filter(matcher, summary);
		} else {
			Summary s;
			querySummaryFromDB("reftime >= '" + begin->toSQL() + "'"
					   " AND reftime < '" + end->toSQL() + "'", s);
			s.filter(matcher, summary);
		}

		// Advance to the beginning of the next month
		begin->vals[0] += (begin->vals[1]/12);
		begin->vals[1] = (begin->vals[1] % 12) + 1;
		begin->vals[2] = 1;
		begin->vals[3] = begin->vals[4] = begin->vals[5] = 0;
	}

	/*
	// Iterate the months
	int year = begin->vals[0];
	int month = begin->vals[1];
	while (year < end->vals[0] || (year == end->vals[0] && month <= end->vals[1]))
	{
		Summary s = summaryForMonth(year, month);
		s.filter(matcher, summary);

		// Increment the month
		month = (month%12) + 1;
		if (month == 1)
			++year;
	}
	*/

	return true;
}


RIndex::RIndex(const ConfigFile& cfg)
   	: Index(cfg)
{
}

RIndex::~RIndex()
{
}

void RIndex::open()
{
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " is already open");

	if (!wibble::sys::fs::access(m_pathname, F_OK))
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " does not exist");
	
	m_db.open(m_pathname);
	setupPragmas();
	
	initQueries();

	if (sys::fs::access(m_root, W_OK))
		sys::fs::mkdirIfMissing(m_scache_root, 0777);
}

void RIndex::initQueries()
{
	Index::initQueries();
}

#if 0
bool RIndex::fetch(const Metadata& md, std::string& file, size_t& ofs)
{
	// TODO: acquire all the attr table IDs for md
	// TODO: query those plus all the non-summarisable metadata
	// TODO: find the matching line
#if 0
	// SELECT file, ofs FROM md WHERE id=id
	string mdid = id(md);
	m_fetch_by_id.reset();
	m_fetch_by_id.bind(1, mdid);
	if (!m_fetch_by_id.step())
		return false;

	// int iid = m_fetch_by_id.fetchInt(0);
	file = m_fetch_by_id.fetchString(1);
	ofs = m_fetch_by_id.fetchSizeT(2);

	// Reset the query to close the statement
	m_fetch_by_id.reset();

	return true;
#endif
}
#endif

WIndex::WIndex(const ConfigFile& cfg)
	: Index(cfg), m_insert(m_db),
          m_delete("delete", m_db), m_replace("replace", m_db)
{
}

WIndex::~WIndex()
{
}

bool WIndex::open()
{
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " is already open");

	bool need_create = !wibble::sys::fs::access(m_pathname, F_OK);

	m_db.open(m_pathname);
	setupPragmas();
	
	if (need_create)
		initDB();

	initQueries();

	sys::fs::mkdirIfMissing(m_scache_root, 0777);

	return need_create;
}

void WIndex::initQueries()
{
	Index::initQueries();

	// Precompile insert query
	string un_ot;
	string placeholders;
	if (m_uniques)
	{
		un_ot += ", uniq";
		placeholders += ", ?";
	}
	if (m_others)
	{
		un_ot += ", other";
		placeholders += ", ?";
	}

	// Precompile insert
	m_insert.compile("INSERT INTO md (format, file, offset, size, notes, reftime" + un_ot + ")"
			 " VALUES (?, ?, ?, ?, ?, ?" + placeholders + ")");

	// Precompile replace
	m_replace.compile("INSERT OR REPLACE INTO md (format, file, offset, size, notes, reftime" + un_ot + ")"
		          " VALUES (?, ?, ?, ?, ?, ?" + placeholders + ")");

	// Precompile remove query
	m_delete.compile("DELETE FROM md WHERE id=?");
}

void WIndex::initDB()
{
	if (m_uniques) m_uniques->initDB(m_components_indexed);
	if (m_others) m_others->initDB(m_components_indexed);

	// Create the main table
	string query = "CREATE TABLE IF NOT EXISTS md ("
		"id INTEGER PRIMARY KEY,"
		" format TEXT NOT NULL,"
		" file TEXT NOT NULL,"
		" offset INTEGER NOT NULL,"
		" size INTEGER NOT NULL,"
		" notes BLOB,"
		" reftime TEXT NOT NULL";
	if (m_uniques) query += ", uniq INTEGER NOT NULL";
	if (m_others) query += ", other INTEGER NOT NULL";
	if (m_uniques)
		query += ", UNIQUE(reftime, uniq)";
	else
		query += ", UNIQUE(reftime)";
	query += ")";
	m_db.exec(query);

	// Create indices on the main table
	m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
	m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_file ON md (file)");
	if (m_uniques) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_uniq ON md (uniq)");
	if (m_others) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_other ON md (other)");
}

void WIndex::bindInsertParams(Query& q, Metadata& md, const std::string& file, size_t ofs, char* timebuf)
{
	int idx = 0;

	q.bind(++idx, md.source->format);
	q.bind(++idx, file);
	q.bind(++idx, ofs);
	switch (md.source->style())
	{
		case Source::BLOB:
			q.bind(++idx, md.source.upcast<source::Blob>()->size);
			break;
		case Source::INLINE:
			q.bind(++idx, md.source.upcast<source::Inline>()->size);
			break;
		default:
			q.bind(++idx, 0);
	}
	//q.bindItems(++idx, md.notes);
	q.bind(++idx, md.notes_encoded());

	const int* rt = md.get(types::TYPE_REFTIME)->upcast<types::reftime::Position>()->time->vals;
	int len = snprintf(timebuf, 25, "%04d-%02d-%02d %02d:%02d:%02d", rt[0], rt[1], rt[2], rt[3], rt[4], rt[5]);
	q.bind(++idx, timebuf, len);

	if (m_uniques)
	{
		int id = m_uniques->obtain(md);
		q.bind(++idx, id);
	}
	if (m_others)
	{
		int id = m_others->obtain(md);
		q.bind(++idx, id);
	}
}

Pending WIndex::beginTransaction()
{
	return Pending(new SqliteTransaction(m_db));
}

Pending WIndex::beginExclusiveTransaction()
{
	return Pending(new SqliteTransaction(m_db, true));
}

void WIndex::index(Metadata& md, const std::string& file, size_t ofs, int* id)
{
	m_insert.reset();

	char buf[25];
	bindInsertParams(m_insert, md, file, ofs, buf);

	while (m_insert.step())
		;

	if (id)
		*id = m_db.lastInsertID();

	// Invalidate the summary cache for this month
	invalidateSummaryCache(md);
}

void WIndex::replace(Metadata& md, const std::string& file, size_t ofs, int* id)
{
	m_replace.reset();

	char buf[25];
	bindInsertParams(m_replace, md, file, ofs, buf);

	while (m_replace.step())
		;

	if (id)
		*id = m_db.lastInsertID();

	// Invalidate the summary cache for this month
	invalidateSummaryCache(md);
}

void WIndex::remove(int id, std::string& file)
{
	Query fetch_by_id("byid", m_db);
	fetch_by_id.compile("SELECT file, reftime FROM md WHERE id=?");
	fetch_by_id.bind(1, id);
	string reftime;
	while (fetch_by_id.step())
	{
		file = fetch_by_id.fetchString(0);
		reftime = fetch_by_id.fetchString(1);
	}
	if (reftime.empty())
		throw wibble::exception::Consistency(
				"removing item with id " + str::fmt(id),
				"id does not exist in the index");

	// Invalidate the summary cache for this month
	Item<types::Time> rt = types::Time::createFromSQL(reftime);
	invalidateSummaryCache(rt->vals[0], rt->vals[1]);

	// DELETE FROM md WHERE id=?
	m_delete.reset();
	m_delete.bind(1, id);
	while (m_delete.step())
		;
}

void WIndex::reset()
{
	m_db.exec("DELETE FROM md");
}

void WIndex::reset(const std::string& file)
{
	Query query("reset_datafile", m_db);
	query.compile("DELETE FROM md WHERE file = ?");
	query.bind(1, file);
	query.step();
}

void WIndex::relocate_data(int id, off_t newofs)
{
	Query query("update_offset", m_db);
	query.compile("UPDATE md SET offset = ? WHERE id = ?");
	query.bind(1, newofs);
	query.bind(2, id);
	while (query.step())
		;
}

void WIndex::vacuum()
{
	m_db.exec("PRAGMA journal_mode = TRUNCATE");
	if (m_uniques)
		m_db.exec("delete from mduniq where id not in (select uniq from md)");
	if (m_others)
		m_db.exec("delete from mdother where id not in (select other from md)");
	m_db.exec("VACUUM");
	m_db.exec("ANALYZE");
	m_db.exec("PRAGMA journal_mode = PERSIST");
}

}
}
}
// vim:set ts=4 sw=4:
