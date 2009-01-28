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

#include <arki/dataset/index.h>
#include <arki/dataset/ondisk/fetcher.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/dataset.h>
#include <arki/types/reftime.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <ctime>
#include <cassert>

#define LEGACY_SQLITE

// FIXME: for debugging
//#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::dataset::index;

namespace arki {
namespace dataset {

namespace index {
bool InsertQuery::step()
{
	int rc = sqlite3_step(m_stm);
	if (rc != SQLITE_DONE)
	{
#ifdef LEGACY_SQLITE
		rc = sqlite3_reset(m_stm);
#endif
		// Different exception for duplicate inserts, to be able to treat
		// duplicate inserts differently
		if (rc == SQLITE_CONSTRAINT)
			throw DSIndex::DuplicateInsert("executing " + name + " query");
		else
			m_db.throwException("executing " + name + " query");
	}
	return false;
}
}

DSIndex::DSIndex(const ConfigFile& cfg)
	: m_root(cfg.value("path")), m_insert(m_db),
	  m_fetch_by_id("fetch by id", m_db), m_delete("delete", m_db),
	  m_replace("replace", m_db), m_committer(m_db), m_index_reftime(false)
{
	m_indexpath = cfg.value("indexfile");
	if (m_indexpath.empty())
		m_indexpath = "index.sqlite";

	// What metadata components we use to create a unique id
	string unique = cfg.value("unique");
	if (unique.empty())
		unique = "reftime, origin, product, level, timerange, area";
	m_id_maker.init(unique);

	// What metadata components we index
	string index = cfg.value("index");
	if (index.empty())
		index = "reftime";
	m_components_indexed = parseMetadataBitmask(index);

	// Instantiate subtables
	for (set<types::Code>::const_iterator i = m_components_indexed.begin();
			i != m_components_indexed.end(); ++i)
		switch (*i)
		{
			case types::TYPE_REFTIME: m_index_reftime = true; break;
			default: m_sub.insert(make_pair(*i, new AttrSubIndex(m_db, *i))); break;
		}
}

DSIndex::~DSIndex()
{
	for (std::map<types::Code, index::AttrSubIndex*>::iterator i = m_sub.begin();
			i != m_sub.end(); ++i)
		delete i->second;
}

void DSIndex::metadataQuery(const std::string& query, MetadataConsumer& consumer) const
{
	ondisk::Fetcher fetcher(m_root);
	sqlite3_stmt* stm_query = m_db.prepare(query);

	// TODO: see if it's worth sorting file and offset

	int res;
	while ((res = sqlite3_step(stm_query)) == SQLITE_ROW)
	{
		// fetch the Metadata and pass it to consumer
		fetcher.fetch(
			(const char*)sqlite3_column_text(stm_query, 0),
			sqlite3_column_int(stm_query, 1),
			consumer);
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

void DSIndex::bindInsertParams(Query& q, Metadata& md, const std::string& mdid, const std::string& file, size_t ofs, char* timebuf)
{
	int idx = 0;

	q.bind(++idx, mdid);
	q.bind(++idx, file);
	q.bind(++idx, ofs);

	if (m_index_reftime)
	{
		const int* rt = md.get(types::TYPE_REFTIME)->upcast<types::reftime::Position>()->time->vals;
		int len = snprintf(timebuf, 25, "%04d-%02d-%02d %02d:%02d:%02d", rt[0], rt[1], rt[2], rt[3], rt[4], rt[5]);
		q.bind(++idx, timebuf, len);
	}
	for (std::map<types::Code, index::AttrSubIndex*>::iterator i = m_sub.begin();
			i != m_sub.end(); ++i)
	{
		int id = i->second->insert(md);
		if (id != -1)
			q.bind(++idx, id);
		else
			q.bindNull(++idx);
	}
}


void DSIndex::open()
{
	string pathname = m_root.empty() ? m_indexpath : m_root + "/" + m_indexpath;

	if (m_db.isOpen())
		throw wibble::exception::Consistency("opening dataset index", "index " + pathname + " is already open");

	bool need_init = !wibble::sys::fs::access(pathname, F_OK);
	//cerr << "Needs init for " << pathname << ": " << need_init << endl;
	
	m_db.open(pathname);

	// We don't need synchronous writes, since we have our own flagfile to
	// detect a partially writte index
	m_db.exec("PRAGMA synchronous = OFF");
	// For the same reason, we don't need an on-disk rollback journal: if we
	// crash, our flagfile will be there
	m_db.exec("PRAGMA journal_mode = MEMORY");
	// Also, since the way we do inserts cause no trouble if a reader reads a
	// partial insert, we do not need read locking
	m_db.exec("PRAGMA read_uncommitted = 1");

	if (need_init)
		initDB();
	
	initQueries();
}

void DSIndex::initDB()
{
	string query = "CREATE TABLE IF NOT EXISTS md ("
		"id INTEGER PRIMARY KEY,"
		" mdid TEXT NOT NULL,"
		" file TEXT NOT NULL,"
		" offset INTEGER NOT NULL";

	if (m_index_reftime)
		query += ", reftime TEXT NOT NULL";

	for (std::map<types::Code, index::AttrSubIndex*>::iterator i = m_sub.begin();
			i != m_sub.end(); ++i)
	{
		query += ", " + i->second->name + " INTEGER";
		i->second->initDB();
	}
	query += ", UNIQUE(mdid))";

	m_db.exec(query);

	// Create the indexes
	if (m_index_reftime)
		m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
	for (std::map<types::Code, index::AttrSubIndex*>::iterator i = m_sub.begin();
			i != m_sub.end(); ++i)
	{
		m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_" + i->second->name
							+ " ON md (" + i->second->name + ")");
	}
}

void DSIndex::initQueries()
{
	m_committer.initQueries();

	// Precompile insert query
	string query;
	int fieldCount = 0;
	if (m_index_reftime)
	{
		query += ", reftime";
		++fieldCount;
	}
	for (std::map<types::Code, index::AttrSubIndex*>::iterator i = m_sub.begin();
			i != m_sub.end(); ++i)
	{
		query += ", " + i->second->name; ++fieldCount;
		i->second->initQueries();
	}
	query += ") VALUES (?, ?, ?";
	while (fieldCount--)
		query += ", ?";
	query += ")";

	// Precompile insert
	m_insert.compile("INSERT INTO md (mdid, file, offset" + query);

	// Precompile replace
	m_replace.compile("INSERT OR REPLACE INTO md (mdid, file, offset" + query);

	// Precompile fetch by ID query
	m_fetch_by_id.compile("SELECT id, file, offset FROM md WHERE mdid=?");

	// Precompile remove query
	m_delete.compile("DELETE FROM md WHERE id=?");
}

std::string DSIndex::id(const Metadata& md) const
{
	return m_id_maker.id(md);
}

Pending DSIndex::beginTransaction()
{
	return Pending(new SqliteTransaction(m_committer));
}

bool DSIndex::fetch(const Metadata& md, std::string& file, size_t& ofs)
{
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
}

bool DSIndex::query(const Matcher& m, MetadataConsumer& consumer) const
{
	string query = "SELECT file, offset FROM md WHERE ";
	int found = 0;

	// Split m in what can be SQL-queried and what cannot
	Matcher mine;
	Matcher others;
	m.split(m_components_indexed, mine, others);

	if (mine.empty())
		// There's no part of the matcher that can use the index: return false
		// to let the caller use another query strategy
		return false;

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
					std::map<types::Code, index::AttrSubIndex*>::const_iterator q = m_sub.find(i->first);
					assert(q != m_sub.end());

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

	// If we have not found any match expressions that can use the index,
	// stop here and let the caller deal with it
	if (!found)
		return false;

	if (others.empty())
	{
		if (m_index_reftime)
			query += " ORDER BY reftime";
		// If no non-SQL matcher comes out, then pipe direct to consumer
		metadataQuery(query, consumer);
	} else {
		// Else add a filtering step to the consumer using 'others'
		FilteredMetadataConsumer fc(others, consumer);
		metadataQuery(query, fc);
	}

	return true;
}

void DSIndex::index(Metadata& md, const std::string& file, size_t ofs)
{
	string mdid = id(md);

	m_insert.reset();

	char buf[25];
	bindInsertParams(m_insert, md, mdid, file, ofs, buf);

	m_insert.step();
}

void DSIndex::remove(const std::string& id, std::string& file, size_t& ofs)
{
	// SELECT file, ofs FROM md WHERE id=id
	m_fetch_by_id.reset();
	m_fetch_by_id.bind(1, id);
	if (!m_fetch_by_id.step())
		// No results, nothing to delete
		return;

	int iid = m_fetch_by_id.fetchInt(0);
	file = m_fetch_by_id.fetchString(1);
	ofs = m_fetch_by_id.fetchSizeT(2);

	// Reset the query to close the statement
	m_fetch_by_id.reset();

	// DELETE FROM md WHERE id=?
	m_delete.reset();
	m_delete.bind(1, iid);
	m_delete.step();
}

void DSIndex::replace(Metadata& md, const std::string& file, size_t ofs)
{
	string mdid = id(md);

	m_replace.reset();

	char buf[25];
	bindInsertParams(m_replace, md, mdid, file, ofs, buf);

	m_replace.step();
}

void DSIndex::reset()
{
	m_db.exec("DELETE FROM md");
}

void DSIndex::reset(const std::string& file)
{
	Query query("reset_datafile", m_db);
	query.compile("DELETE FROM md WHERE file = ?");
	query.bind(1, file);
	query.step();
}

}
}
// vim:set ts=4 sw=4:
