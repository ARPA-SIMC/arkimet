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

#include <arki/dataset/ondisk/index.h>
#include <arki/dataset/ondisk/fetcher.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/dataset.h>
#include <arki/types/reftime.h>
#include <arki/utils/metadata.h>

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
using namespace arki::utils::sqlite;
using namespace arki::dataset::index;

namespace arki {
namespace dataset {
namespace ondisk {

Index::Index(const ConfigFile& cfg)
	: m_root(cfg.value("path")),
	  m_index_reftime(false)
{
	m_indexpath = cfg.value("indexfile");
	if (m_indexpath.empty())
		m_indexpath = "index.sqlite";

	m_pathname = m_root.empty() ? m_indexpath : str::joinpath(m_root, m_indexpath);

	// What metadata components we use to create a unique id
	string unique = cfg.value("unique");
	if (unique.empty())
		unique = "reftime, origin, product, level, timerange, area";
	m_id_maker.components = parseMetadataBitmask(unique);

	// What metadata components we index
	string index = cfg.value("index");
	if (index.empty())
		index = "reftime";
	m_components_indexed = parseMetadataBitmask(index);
}

Index::~Index()
{
}

RIndex::RIndex(const ConfigFile& cfg)
       	: Index(cfg),
	  m_fetch_by_id("fetch by id", m_db)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = m_components_indexed.begin();
			i != m_components_indexed.end(); ++i)
		switch (*i)
		{
			case types::TYPE_REFTIME: m_index_reftime = true; break;
			default: m_rsub.insert(make_pair(*i, new RAttrSubIndex(m_db, *i))); break;
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
	// We don't need synchronous writes, since we have our own flagfile to
	// detect a partially written index
	m_db.exec("PRAGMA synchronous = OFF");
	// For the same reason, we don't need an on-disk rollback journal: if we
	// crash, our flagfile will be there
	m_db.exec("PRAGMA journal_mode = MEMORY");
	// Also, since the way we do inserts cause no trouble if a reader reads a
	// partial insert, we do not need read locking
	m_db.exec("PRAGMA read_uncommitted = 1");

	// Precompile fetch by ID query
	m_fetch_by_id.compile("SELECT id, file, offset FROM md WHERE mdid=?");

	for (std::map<types::Code, index::RAttrSubIndex*>::iterator i = m_rsub.begin();
			i != m_rsub.end(); ++i)
		i->second->initQueries();
}

std::string RIndex::id(const Metadata& md) const
{
	return m_id_maker.id(md);
}

bool RIndex::fetch(const Metadata& md, std::string& file, size_t& ofs)
{
	// SELECT file, ofs FROM md WHERE id=id
	string mdid = id(md);
	m_fetch_by_id.reset();
	m_fetch_by_id.bind(1, mdid);
	if (!m_fetch_by_id.step())
		return false;

	// int iid = m_fetch_by_id.fetchInt(0);
	file = m_fetch_by_id.fetchString(1);
	ofs = m_fetch_by_id.fetch<size_t>(2);

	// Reset the query to close the statement
	m_fetch_by_id.reset();

	return true;
}

void RIndex::metadataQuery(const std::string& query, MetadataConsumer& consumer) const
{
	ondisk::Fetcher fetcher(m_root);
	utils::metadata::Collector mdbuf;
	{
		Query mdq("mdq", m_db);
		mdq.compile(query);

		// TODO: see if it's worth sorting file and offset

		while (mdq.step())
		{
			// fetch the Metadata and buffer it in memory, to release the
			// database lock as soon as possible
			fetcher.fetch(
				mdq.fetchString(0),
				mdq.fetch<int>(1),
				mdbuf);
		}
	}

	// pass it to consumer
	for (utils::metadata::Collector::iterator i = mdbuf.begin();
			i != mdbuf.end(); ++i)
		consumer(*i);
}

bool RIndex::query(const Matcher& m, MetadataConsumer& consumer) const
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


WIndex::WIndex(const ConfigFile& cfg)
	: RIndex(cfg), m_insert(m_db),
          m_delete("delete", m_db), m_replace("replace", m_db)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = m_components_indexed.begin();
			i != m_components_indexed.end(); ++i)
		switch (*i)
		{
			case types::TYPE_REFTIME: m_index_reftime = true; break;
			default: m_wsub.insert(make_pair(*i, new WAttrSubIndex(m_db, *i))); break;
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

	// Precompile insert query
	string query;
	int fieldCount = 0;
	if (m_index_reftime)
	{
		query += ", reftime";
		++fieldCount;
	}
	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
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

	// Precompile remove query
	m_delete.compile("DELETE FROM md WHERE id=?");
}

void WIndex::initDB()
{
	string query = "CREATE TABLE IF NOT EXISTS md ("
		"id INTEGER PRIMARY KEY,"
		" mdid TEXT NOT NULL,"
		" file TEXT NOT NULL,"
		" offset INTEGER NOT NULL";

	if (m_index_reftime)
		query += ", reftime TEXT NOT NULL";

	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		query += ", " + i->second->name + " INTEGER";
		i->second->initDB();
	}
	query += ", UNIQUE(mdid))";

	m_db.exec(query);

	// Create the indexes
	if (m_index_reftime)
		m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
	for (std::map<types::Code, index::WAttrSubIndex*>::iterator i = m_wsub.begin();
			i != m_wsub.end(); ++i)
	{
		m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_" + i->second->name
							+ " ON md (" + i->second->name + ")");
	}
}

void WIndex::bindInsertParams(Query& q, Metadata& md, const std::string& mdid, const std::string& file, size_t ofs, char* timebuf)
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
	return Pending(new SqliteTransaction(m_db));
}

void WIndex::index(Metadata& md, const std::string& file, size_t ofs)
{
	string mdid = id(md);

	m_insert.reset();

	char buf[25];
	bindInsertParams(m_insert, md, mdid, file, ofs, buf);

	m_insert.step();
}

void WIndex::remove(const std::string& id, std::string& file, size_t& ofs)
{
	// SELECT file, ofs FROM md WHERE id=id
	m_fetch_by_id.reset();
	m_fetch_by_id.bind(1, id);
	if (!m_fetch_by_id.step())
		// No results, nothing to delete
		return;

	int iid = m_fetch_by_id.fetch<int>(0);
	file = m_fetch_by_id.fetchString(1);
	ofs = m_fetch_by_id.fetch<size_t>(2);

	// Reset the query to close the statement
	m_fetch_by_id.reset();

	// DELETE FROM md WHERE id=?
	m_delete.reset();
	m_delete.bind(1, iid);
	m_delete.step();
}

void WIndex::replace(Metadata& md, const std::string& file, size_t ofs)
{
	string mdid = id(md);

	m_replace.reset();

	char buf[25];
	bindInsertParams(m_replace, md, mdid, file, ofs, buf);

	m_replace.step();
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
