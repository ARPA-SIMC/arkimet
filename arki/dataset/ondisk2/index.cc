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
#include <arki/utils/files.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <sstream>
#include <ctime>
#include <cassert>

// FIXME: for debugging
//#include <iostream>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
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
	  m_fetch_by_id("byid", m_db),
	  m_get_id("getid", m_db),
	  m_uniques(0), m_others(0)
{
	m_indexpath = cfg.value("indexfile");
	if (m_indexpath.empty())
		m_indexpath = "index.sqlite";

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

	m_fetch_by_id.compile("SELECT format, file, offset, size FROM md WHERE id=?");

	string query = "SELECT id FROM md WHERE reftime=?";
	if (m_uniques) query += " AND uniq=?";
	if (m_others) query += " AND other=?";
	m_get_id.compile(query);
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
		id = m_get_id.fetchInt(0);

	return id;
}

size_t Index::count() const
{
	Query sq("count", m_db);
	sq.compile("SELECT COUNT(*) FROM md");
	size_t res = 0;
	while (sq.step())
		res = sq.fetchSizeT(0);
	return res;
}

void Index::scan_files(writer::IndexFileVisitor& v, const std::string& orderBy) const
{
	Query sq("scan_files", m_db);
	sq.compile("SELECT id, file, offset, size FROM md ORDER BY " + orderBy);
	while (sq.step())
	{
		int id = sq.fetchInt(0);
		string file = sq.fetchString(1);
		off_t offset = sq.fetchSizeT(2);
		size_t size = sq.fetchSizeT(3);

		v(file, id, offset, size);
	}
}

void Index::scan_file(const std::string& relname, writer::IndexFileVisitor& v, const std::string& orderBy) const
{
	Query sq("scan_file", m_db);
	sq.compile("SELECT id, offset, size FROM md WHERE file=? ORDER BY " + orderBy);
	sq.bind(1, relname);
	while (sq.step())
	{
		int id = sq.fetchInt(0);
		off_t offset = sq.fetchSizeT(1);
		size_t size = sq.fetchSizeT(2);
		v(relname, id, offset, size);
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
			if (m_uniques->add_constraints(m, constraints, "u"))
				query += " JOIN mduniq AS u ON uniq = u.id";

		if (m_others)
			if (m_others->add_constraints(m, constraints, "o"))
				query += " JOIN mdother AS o ON other = o.id";
	}

	if (not m.empty() and constraints.size() != m.m_impl->size())
		// We can only see what we index, the rest is lost
		// TODO: add a filter to the query results, before the
		//       postprocessor that pulls in data from disk
		return false;

	if (!constraints.empty())
		query += " WHERE " + str::join(constraints.begin(), constraints.end(), " AND ");
	return true;
}

bool Index::query(const Matcher& m, MetadataConsumer& consumer) const
{
	string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";

	if (m_uniques) query += ", m.uniq";
	if (m_others) query += ", m.other";

	query += " FROM md AS m";

	try {
		if (!addJoinsAndConstraints(m, query))
			return false;
	} catch (NotFound) {
		// If one of the subqueries did not find any match, we can directly
		// return true here, as we are not going to get any result
		return true;
	}

	query += " ORDER BY m.reftime";

	vector<Metadata> mdbuf;
	// TODO: if verbose, output the query

	// Limited scope for mdq, so we finalize the query before starting to
	// emit results
	{
		Query mdq("mdq", m_db);
		mdq.compile(query);

		// TODO: see if it's worth sorting mdbuf by file and offset

		while (mdq.step())
		{
			// Rebuild the Metadata
			Metadata md;
			md.create();
			md.set(types::AssignedDataset::create(m_name, str::fmt(mdq.fetchInt(0))));
			md.source = source::Blob::create(
					mdq.fetchString(1), mdq.fetchString(2),
					mdq.fetchInt(3), mdq.fetchInt(4));
			md.notes = mdq.fetchItems<types::Note>(5);
			md.set(reftime::Position::create(Time::createFromSQL(mdq.fetchString(6))));
			int j = 7;
			if (m_uniques)
			{
				if (mdq.fetchType(j) != SQLITE_NULL)
					m_uniques->read(mdq.fetchInt(j), md);
				++j;
			}
			if (m_others)
			{
				if (mdq.fetchType(j) != SQLITE_NULL)
					m_others->read(mdq.fetchInt(j), md);
				++j;
			}

			// Buffer the results in memory, to release the database lock as soon as possible
			mdbuf.push_back(md);
		}
	}

	// pass it to consumer
	for (vector<Metadata>::iterator i = mdbuf.begin();
			i != mdbuf.end(); ++i)
		consumer(*i);

	return true;
}

bool Index::querySummary(const Matcher& m, Summary& summary) const
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

	// TODO: if verbose, output the query

	Query sq("sq", m_db);
	sq.compile(query);

	while (sq.step())
	{
		// Fill in the summary statistics
		arki::Item<summary::Stats> st(new summary::Stats);
		st->count = sq.fetchInt(0);
		st->size = sq.fetchInt(1);
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
				m_uniques->read(sq.fetchInt(j), md);
			++j;
		}
		if (m_others)
		{
			if (sq.fetchType(j) != SQLITE_NULL)
				m_others->read(sq.fetchInt(j), md);
			++j;
		}

		// Feed the results to the summary
		summary.add(md, st);
	}

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

void WIndex::open()
{
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " is already open");

	bool need_create = !wibble::sys::fs::access(m_pathname, F_OK);

	m_db.open(m_pathname);
	setupPragmas();
	
	if (need_create)
		initDB();

	initQueries();
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
	if (m_uniques) query += ", UNIQUE(reftime, uniq)";
	query += ")";
	m_db.exec(query);

	// Create indices on the main table
	m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
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
	q.bindItems(++idx, md.notes);

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

	m_insert.step();

	if (id)
		*id = m_db.lastInsertID();
}

void WIndex::replace(Metadata& md, const std::string& file, size_t ofs, int* id)
{
	m_replace.reset();

	char buf[25];
	bindInsertParams(m_replace, md, file, ofs, buf);

	m_replace.step();

	if (id)
		*id = m_db.lastInsertID();
}

void WIndex::remove(int id, std::string& file)
{
	// SELECT format, file, offset, size FROM md WHERE id=?
	m_fetch_by_id.reset();
	m_fetch_by_id.bind(1, id);
	while (m_fetch_by_id.step())
		file = m_fetch_by_id.fetchString(1);

	// DELETE FROM md WHERE id=?
	m_delete.reset();
	m_delete.bind(1, id);
	m_delete.step();
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
	query.step();
}

void WIndex::vacuum()
{
	m_db.exec("PRAGMA journal_mode = TRUNCATE");
	m_db.exec("VACUUM");
	m_db.exec("ANALYZE");
	m_db.exec("PRAGMA journal_mode = PERSIST");
}

}
}
}
// vim:set ts=4 sw=4:
