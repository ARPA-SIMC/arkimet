/*
 * dataset/index/attr - Generic index for metadata items
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/index/attr.h>
#include <wibble/exception.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils::sqlite;

namespace arki {
namespace dataset {
namespace index {

void GetAttrID::initQueries()
{
	// Build the query
	m_stm = m_db.prepare("SELECT id FROM " + m_table + " WHERE data=?");
}

int GetAttrID::operator()(const std::string& blob) const
{
	// Reset the query
	int res = sqlite3_reset(m_stm);
	if (res != SQLITE_OK)
		m_db.throwException("resetting GetAttrID query for " + m_table);

	// Bind the blob
	sqlite3_bind_blob(m_stm, 1, blob.data(), blob.size(), SQLITE_STATIC);

	// Run the query
	int id = -1;
	while ((res = sqlite3_step(m_stm)) == SQLITE_ROW)
		id = sqlite3_column_int(m_stm, 0);
	if (res != SQLITE_DONE)
	{
#ifdef LEGACY_SQLITE
		sqlite3_reset(m_stm);
#endif
		m_db.throwException("executing GetAttrID query for " + m_table);
	}
	return id;
}

AttrSubIndex::AttrSubIndex(types::Code serCode)
	: name(types::tag(serCode)), serCode(serCode)
{
}

AttrSubIndex::~AttrSubIndex()
{
}

RAttrSubIndex::RAttrSubIndex(SQLiteDB& db, types::Code serCode)
	: AttrSubIndex(serCode), m_db(db),
          m_select_all("sel_all", db),
	  m_select_one("sel_one", db),
	  m_select_id("sel_id", db)
{
}

RAttrSubIndex::~RAttrSubIndex()
{
}

void RAttrSubIndex::initQueries()
{
	// Compile the select all query
	m_select_all.compile("SELECT id, data FROM sub_" + name);
	m_select_one.compile("SELECT data FROM sub_" + name + " where id=?");
	m_select_id.compile("SELECT id FROM sub_" + name + " where data=?");
}

int RAttrSubIndex::id(const Metadata& md) const
{
	UItem<> item = md.get(serCode);

	// First look up in cache
	for (std::map< int, UItem<> >::const_iterator i = m_cache.begin();
			i != m_cache.end(); ++i)
		if (i->second == item)
			return i->first;

	// Else, fetch it from the database
	m_select_id.reset();
	m_select_id.bind(1, item.encode());
	int id = -1;
	while (m_select_id.step())
		id = m_select_id.fetchInt(0);

	// Add it to the cache
	if (id != -1)
		m_cache.insert(make_pair(id, item));

	return id;
}

void RAttrSubIndex::read(int id, Metadata& md) const
{
	std::map< int, UItem<> >::const_iterator i = m_cache.find(id);
	if (i != m_cache.end())
	{
		md.set(i->second);
		return;
	}

	// Reset the query
	m_select_one.reset();
	m_select_one.bind(1, id);

	// Decode every blob and run the matcher on it
	if (m_select_one.step())
	{
		const void* buf = m_select_one.fetchBlob(0);
		int len = m_select_one.fetchBytes(0);
		Item<> i = types::decodeInner((types::Code)serCode, (const unsigned char*)buf, len);
		m_cache.insert(make_pair(id, i));
		md.set(i);
	}
}

std::vector<int> RAttrSubIndex::query(const matcher::OR& m) const
{
	std::vector<int> ids;
	// Reset the query
	m_select_all.reset();

	// Decode every blob and run the matcher on it
	while (m_select_all.step())
	{
		const void* buf = m_select_all.fetchBlob(1);
		int len = m_select_all.fetchBytes(1);
		Item<> t = types::decodeInner((types::Code)serCode, (const unsigned char*)buf, len);
		if (m.matchItem(t))
			ids.push_back(m_select_all.fetchInt(0));
	}
	return ids;
}

WAttrSubIndex::WAttrSubIndex(SQLiteDB& db, types::Code serCode)
	: AttrSubIndex(serCode), m_db(db),
	  m_get_blob_id(db, "sub_" + name), m_stm_insert(0)
{
}

WAttrSubIndex::~WAttrSubIndex()
{
	if (m_stm_insert) sqlite3_finalize(m_stm_insert);
}

void WAttrSubIndex::initDB()
{
	// Create the table
	std::string query = "CREATE TABLE IF NOT EXISTS sub_" + name + " ("
		"id INTEGER PRIMARY KEY,"
		" data BLOB NOT NULL,"
		" UNIQUE(data))";
	m_db.exec(query);
}

void WAttrSubIndex::initQueries()
{
	m_get_blob_id.initQueries();

	// Compile the insert query
	m_stm_insert = m_db.prepare("INSERT INTO sub_" + name + " (data) VALUES (?)");
}

int WAttrSubIndex::insert(const Metadata& md)
{
	UItem<> item = md.get(serCode);
	if (!item.defined())
		return -1;

	// Extract the blob to insert
	std::string blob = item->encodeWithoutEnvelope();

	// Try to serve it from cache of possible
	std::map<std::string, int>::const_iterator ci = m_id_cache.find(blob);
	if (ci != m_id_cache.end())
		return ci->second;

	// Check if we already have the blob in the database
	int id = m_get_blob_id(blob);
	if (id != -1)
		return m_id_cache[blob] = id;
	else
	{
		// Insert it

		// Reset the query
		int rc;
		rc = sqlite3_reset(m_stm_insert);
		if (rc != SQLITE_OK)
			m_db.throwException("resetting INSERT query for sub_" + name);

		// Rebind parameters
		sqlite3_bind_blob(m_stm_insert, 1, blob.data(), blob.size(), SQLITE_STATIC);

		// Perform the query
		rc = sqlite3_step(m_stm_insert);
		if (rc != SQLITE_DONE)
		{
#ifdef LEGACY_SQLITE
			sqlite3_reset(m_stm_insert);
#endif
			m_db.throwException("executing INSERT query for sub_" + name);
		}

		return m_id_cache[blob] = m_db.lastInsertID();
	}
}

}
}
}
// vim:set ts=4 sw=4:
