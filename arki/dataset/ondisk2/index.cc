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
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/dataset.h>
#include <arki/types/reftime.h>
#include <arki/types/source.h>
#include <arki/types/assigneddataset.h>
#include <arki/summary.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <sstream>
#include <ctime>
#include <cassert>

#define LEGACY_SQLITE

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

Index::Index(const ConfigFile& cfg)
	: m_name(cfg.value("name")), m_root(cfg.value("path"))
{
	m_indexpath = cfg.value("indexfile");
	if (m_indexpath.empty())
		m_indexpath = "index.sqlite";

	m_pathname = m_root.empty() ? m_indexpath : str::joinpath(m_root, m_indexpath);

	// What metadata components we use to create a unique id
	string unique = cfg.value("unique");
	if (unique.empty())
		unique = "reftime, origin, product, level, timerange, area";
	m_components_unique = parseMetadataBitmask(unique);

	// What metadata components we index
	string index = cfg.value("index");
	if (index.empty())
		index = "origin, product, level, timerange, area, ensemble, run";
	m_components_indexed = parseMetadataBitmask(index);
	// Add the non-summarisable items that we index in special-cased ways
	m_components_indexed.insert(types::TYPE_REFTIME);
}

Index::~Index()
{
}

RIndex::RIndex(const ConfigFile& cfg)
   	: Index(cfg), m_get_id("getid", m_db), m_fetch_by_id("byid", m_db)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = m_components_indexed.begin();
			i != m_components_indexed.end(); ++i)
	{
		if (*i == types::TYPE_REFTIME) continue;
		m_rsub.insert(make_pair(*i, new RAttrSubIndex(m_db, *i)));
	}
}

RIndex::~RIndex()
{
	for (std::map<types::Code, index::RAttrSubIndex*>::iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		delete i->second;
}

void RIndex::open()
{
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " is already open");

	if (!wibble::sys::fs::access(m_pathname, F_OK))
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " does not exist");
	
	m_db.open(m_pathname);
	
	initQueries();
}

void RIndex::initQueries()
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

	for (std::map<types::Code, index::RAttrSubIndex*>::iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		i->second->initQueries();

	string query = "SELECT id FROM md WHERE reftime=?";
	for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		query += " AND " + str::tolower(types::formatCode(i->first)) + "=?";
	m_get_id.compile(query);

	m_fetch_by_id.compile("SELECT format, file, offset, size FROM md WHERE id=?");
}

int RIndex::id(const Metadata& md) const
{
	m_get_id.reset();

	int idx = 0;
	UItem<> urt = md.get(types::TYPE_REFTIME);
	if (!urt.defined()) return -1;
	Item<types::Reftime> rt(urt.upcast<types::Reftime>());
	if (rt->style() != types::Reftime::POSITION) return -1;
	m_get_id.bind(++idx, rt.upcast<types::reftime::Position>()->time->toSQL());

	for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
	{
		int attr_id = i->second->id(md);
		// If we do not have this attribute, then we do not have this metadata
		if (attr_id == -1) return -1;
		m_get_id.bind(++idx, attr_id);
	}

	int id = -1;
	while (m_get_id.step())
		id = m_get_id.fetchInt(0);

	return id;
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

void RIndex::metadataQuery(const std::string& query, MetadataConsumer& consumer) const
{
	sqlite3_stmt* stm_query = m_db.prepare(query);

	// TODO: see if it's worth sorting file and offset

	int res;
	while ((res = sqlite3_step(stm_query)) == SQLITE_ROW)
	{
		// Rebuild the Metadata
		Metadata md;
		md.create();
		md.set(types::AssignedDataset::create(m_name, str::fmt(sqlite3_column_int(stm_query, 0))));
		md.source = source::Blob::create(
			(const char*)sqlite3_column_text(stm_query, 1),
			(const char*)sqlite3_column_text(stm_query, 2),
			sqlite3_column_int(stm_query, 3),
			sqlite3_column_int(stm_query, 4));
		md.set(reftime::Position::create(Time::createFromSQL(
			(const char*)sqlite3_column_text(stm_query, 5))));
		int j = 6;
		for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
				i != m_rsub.end(); ++i, ++j)
			if (sqlite3_column_type(stm_query, j) != SQLITE_NULL)
				i->second->read(sqlite3_column_int(stm_query, j), md);

		// TODO: missing: notes

		// pass it to consumer
		consumer(md);
	}
	if (res != SQLITE_DONE)
	{
#ifdef LEGACY_SQLITE
		/* int rc = */ sqlite3_reset(stm_query);
#endif
		try {
			m_db.throwException("executing query " + query);
		} catch (...) {
			sqlite3_finalize(stm_query);
			throw;
		}
	}
	sqlite3_finalize(stm_query);
}

bool RIndex::query(const Matcher& m, MetadataConsumer& consumer) const
{
	string query = "SELECT id, format, file, offset, size, reftime";
	int found = 0;

	for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		query += ", " + str::tolower(types::formatCode(i->first));

	query += " FROM md";
	// Split m in what can be SQL-queried and what cannot
	Matcher mine;
	Matcher others;
	m.split(m_components_indexed, mine, others);

	if (not others.empty())
		// We can only see what we index, the rest is lost
		// TODO: add a filter to the query results, before the
		//       postprocessor that pulls in data from disk
		return false;

	if (!mine.empty())
	{
		query += " WHERE ";

		try {
			for (matcher::AND::const_iterator i = mine->begin(); i != mine->end(); ++i)
				switch (i->first)
				{
					case types::TYPE_REFTIME: {
						if (found) query += " AND ";
						const matcher::OR* rt = i->second->upcast<matcher::OR>();
						query += "(";
						for (matcher::OR::const_iterator j = rt->begin(); j != rt->end(); ++j)
						{
							if (j != rt->begin())
								query += " OR ";
							query += (*j)->upcast<matcher::MatchReftime>()->sql("reftime");
						}
						query += ")";
						++found;
						break;
					}
					default: {
						std::map<types::Code, index::RAttrSubIndex*>::const_iterator q = m_rsub.find(i->first);
						assert(q != m_rsub.end());

						if (found) query += " AND";
						query += " " + q->second->name
							   + " " + fmtin(q->second->query(*(i->second->upcast<matcher::OR>())));
						++found;
						break;
					}
				}
		} catch (NotFound) {
			// If one of the subqueries did not find any match, we can directly
			// return true here, as we are not going to get any result
			return true;
		}
	}

	query += " ORDER BY reftime";
	metadataQuery(query, consumer);

	return true;
}

void RIndex::summaryQuery(const std::string& query, Summary& summary) const
{
	sqlite3_stmt* stm_query = m_db.prepare(query);

	int res;
	while ((res = sqlite3_step(stm_query)) == SQLITE_ROW)
	{
		// Fill in the summary statistics
		arki::Item<summary::Stats> st(new summary::Stats);
		st->count = sqlite3_column_int(stm_query, 0);
		st->size = sqlite3_column_int(stm_query, 1);
		Item<Time> min_time = Time::createFromSQL((const char*)sqlite3_column_text(stm_query, 2));
		Item<Time> max_time = Time::createFromSQL((const char*)sqlite3_column_text(stm_query, 3));
		st->reftimeMerger.mergeTime(min_time, max_time);

		// Fill in the metadata fields
		Metadata md;
		md.create();
		int j = 4;
		for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
				i != m_rsub.end(); ++i, ++j)
			if (sqlite3_column_type(stm_query, j) != SQLITE_NULL)
				i->second->read(sqlite3_column_int(stm_query, j), md);

		// Feed the results to the summary
		summary.add(md, st);
	}
	if (res != SQLITE_DONE)
	{
#ifdef LEGACY_SQLITE
		/* int rc = */ sqlite3_reset(stm_query);
#endif
		try {
			m_db.throwException("executing query " + query);
		} catch (...) {
			sqlite3_finalize(stm_query);
			throw;
		}
	}
	sqlite3_finalize(stm_query);
}

bool RIndex::querySummary(const Matcher& m, Summary& summary) const
{
	string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";
	int found = 0;

	for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		query += ", " + str::tolower(types::formatCode(i->first));

	query += " FROM md";
	// Split m in what can be SQL-queried and what cannot
	Matcher mine;
	Matcher others;
	m.split(m_components_indexed, mine, others);

	if (not others.empty())
		// We can only see what we index, the rest is lost
		// TODO: add a filter to the query results, before the
		//       postprocessor that pulls in data from disk
		return false;

	if (!mine.empty())
	{
		query += " WHERE ";

		try {
			for (matcher::AND::const_iterator i = mine->begin(); i != mine->end(); ++i)
				switch (i->first)
				{
					case types::TYPE_REFTIME: {
						if (found) query += " AND ";
						const matcher::OR* rt = i->second->upcast<matcher::OR>();
						query += "(";
						for (matcher::OR::const_iterator j = rt->begin(); j != rt->end(); ++j)
						{
							if (j != rt->begin())
								query += " OR ";
							query += (*j)->upcast<matcher::MatchReftime>()->sql("reftime");
						}
						query += ")";
						++found;
						break;
					}
					default: {
						std::map<types::Code, index::RAttrSubIndex*>::const_iterator q = m_rsub.find(i->first);
						assert(q != m_rsub.end());

						if (found) query += " AND";
						query += " " + q->second->name
							   + " " + fmtin(q->second->query(*(i->second->upcast<matcher::OR>())));
						++found;
						break;
					}
				}
		} catch (NotFound) {
			// If one of the subqueries did not find any match, we can directly
			// return true here, as we are not going to get any result
			return true;
		}
	}

	query += " GROUP BY";
	for (std::map<types::Code, index::RAttrSubIndex*>::const_iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
	{
		query += (i == m_rsub.begin()) ? ' ' : ',';
		query += str::tolower(types::formatCode(i->first));
	}

	summaryQuery(query, summary);

	return true;
}

WIndex::WIndex(const ConfigFile& cfg)
	: RIndex(cfg), m_insert(m_db),
          m_delete("delete", m_db), m_replace("replace", m_db),
	  m_committer(m_db)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = m_components_indexed.begin();
			i != m_components_indexed.end(); ++i)
	{
		if (*i == types::TYPE_REFTIME) continue;
		m_wsub.insert(make_pair(*i, new WAttrSubIndex(m_db, *i)));
	}
}

WIndex::~WIndex()
{
	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
		delete i->second;
}

void WIndex::open()
{
	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + m_pathname + " is already open");

	bool need_create = !wibble::sys::fs::access(m_pathname, F_OK);

	m_db.open(m_pathname);
	
	if (need_create)
		initDB();

	initQueries();
}

void WIndex::initQueries()
{
	RIndex::initQueries();

	m_committer.initQueries();

	// Precompile insert query
	string query;
	int fieldCount = 0;
	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		query += ", " + i->second->name;
	   	++fieldCount;
		i->second->initQueries();
	}
	query += ") VALUES (?, ?, ?, ?, ?";
	while (fieldCount--)
		query += ", ?";
	query += ")";

	// Precompile insert
	m_insert.compile("INSERT INTO md (format, file, offset, size, reftime" + query);

	// Precompile replace
	m_replace.compile("INSERT OR REPLACE INTO md (format, file, offset, size, reftime" + query);

	// Precompile remove query
	m_delete.compile("DELETE FROM md WHERE id=?");
}

void WIndex::initDB()
{
	string query = "CREATE TABLE IF NOT EXISTS md ("
		"id INTEGER PRIMARY KEY,"
		" format TEXT NOT NULL,"
		" file TEXT NOT NULL,"
		" offset INTEGER NOT NULL,"
		" size INTEGER NOT NULL,"
		" reftime TEXT NOT NULL";

	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		query += ", " + i->second->name + " INTEGER";
		i->second->initDB();
	}
	query += ", UNIQUE(";
	for (std::set<types::Code>::const_iterator i = m_components_unique.begin();
			i != m_components_unique.end(); ++i)
	{
		if (i != m_components_unique.begin())
			query += ", ";
		query += str::tolower(types::formatCode(*i));
	}
	query += "))";

	m_db.exec(query);

	// Create the indexes
	m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_" + i->second->name
							+ " ON md (" + i->second->name + ")");
	}
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

	const int* rt = md.get(types::TYPE_REFTIME)->upcast<types::reftime::Position>()->time->vals;
	int len = snprintf(timebuf, 25, "%04d-%02d-%02d %02d:%02d:%02d", rt[0], rt[1], rt[2], rt[3], rt[4], rt[5]);
	q.bind(++idx, timebuf, len);

	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		int id = i->second->insert(md);
		if (id != -1)
			q.bind(++idx, id);
		else
			q.bindNull(++idx);
	}
}

Pending WIndex::beginTransaction()
{
	return Pending(new SqliteTransaction(m_committer));
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

}
}
}
// vim:set ts=4 sw=4:
