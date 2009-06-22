/*
 * dataset/index/attr - Generic index for metadata items
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

#include <arki/dataset/index/attr.h>
#include <wibble/exception.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils::sqlite;

namespace arki {
namespace dataset {
namespace index {

int AttrSubIndex::q_select_id(const std::string& blob) const
{
	if (not m_select_id)
	{
		m_select_id = new utils::sqlite::PrecompiledQuery("sel_id", m_db);
		m_select_id->compile("SELECT id FROM sub_" + name + " where data=?");
	}

	// Else, fetch it from the database
	m_select_id->reset();
	m_select_id->bindBlob(1, blob);
	int id = -1;
	while (m_select_id->step())
	{
		id = m_select_id->fetchInt(0);
	}

	return id;
}

UItem<> AttrSubIndex::q_select_one(int id) const
{
	if (not m_select_one)
	{
		m_select_one = new utils::sqlite::PrecompiledQuery("sel_one", m_db);
		m_select_one->compile("SELECT data FROM sub_" + name + " where id=?");
	}

	// Reset the query
	m_select_one->reset();
	m_select_one->bind(1, id);

	// Decode every blob and run the matcher on it
	if (m_select_one->step())
	{
		const void* buf = m_select_one->fetchBlob(0);
		int len = m_select_one->fetchBytes(0);
		return types::decodeInner(code, (const unsigned char*)buf, len);
	}
	return UItem<>();
}

int AttrSubIndex::q_insert(const std::string& blob)
{
	if (not m_insert)
	{
		m_insert = new utils::sqlite::PrecompiledQuery("attr_insert", m_db);
		m_insert->compile("INSERT INTO sub_" + name + " (data) VALUES (?)");
	}

	m_insert->reset();
	m_insert->bindBlob(1, blob);
	m_insert->step();

	return m_db.lastInsertID();
}


AttrSubIndex::AttrSubIndex(utils::sqlite::SQLiteDB& db, types::Code code)
	: name(types::tag(code)), code(code),
	  m_db(db), m_select_id(0), m_select_one(0),
	  m_select_all(0), m_insert(0)
{
}

AttrSubIndex::~AttrSubIndex()
{
	if (m_select_id) delete m_select_id;
	if (m_select_one) delete m_select_one;
	if (m_select_all) delete m_select_all;
	if (m_insert) delete m_insert;
}

int AttrSubIndex::id(const Metadata& md) const
{
	UItem<> item = md.get(code);
	if (!item.defined())
		return -1;

	// First look up in cache
	std::map< Item<>, int >::const_iterator i = m_id_cache.find(item);
	if (i != m_id_cache.end())
		return i->second;

	// Else, fetch it from the database
	int id = q_select_id(item->encodeWithoutEnvelope());

	// Add it to the cache
	if (id != -1)
	{
		m_cache.insert(make_pair(id, item));
		m_id_cache.insert(make_pair(item, id));
	}
	else
		throw NotFound();

	return id;
}

void AttrSubIndex::read(int id, Metadata& md) const
{
	std::map< int, UItem<> >::const_iterator i = m_cache.find(id);
	if (i != m_cache.end())
	{
		md.set(i->second);
		return;
	}

	UItem<> item = q_select_one(id);
	m_cache.insert(make_pair(id, item));
	m_id_cache.insert(make_pair(item, id));
	md.set(item);
}

std::vector<int> AttrSubIndex::query(const matcher::OR& m) const
{
	// Compile the select all query
	if (not m_select_all)
	{
		m_select_all = new utils::sqlite::PrecompiledQuery("sel_all", m_db);
		m_select_all->compile("SELECT id, data FROM sub_" + name);
	}

	std::vector<int> ids;

	// Decode every blob in the database and run the matcher on it
	m_select_all->reset();
	while (m_select_all->step())
	{
		const void* buf = m_select_all->fetchBlob(1);
		int len = m_select_all->fetchBytes(1);
		Item<> t = types::decodeInner(code, (const unsigned char*)buf, len);
		if (m.matchItem(t))
			ids.push_back(m_select_all->fetchInt(0));
	}
	return ids;
}

void AttrSubIndex::initDB()
{
	// Create the table
	std::string query = "CREATE TABLE IF NOT EXISTS sub_" + name + " ("
		"id INTEGER PRIMARY KEY,"
		" data BLOB NOT NULL,"
		" UNIQUE(data))";
	m_db.exec(query);
}

int AttrSubIndex::insert(const Metadata& md)
{
	UItem<> item = md.get(code);
	if (!item.defined())
		return -1;

	// Try to serve it from cache if possible
	std::map<Item<>, int>::const_iterator ci = m_id_cache.find(item);
	if (ci != m_id_cache.end())
		return ci->second;

	// Extract the blob to insert
	std::string blob = item->encodeWithoutEnvelope();

	// Check if we already have the blob in the database
	int id = q_select_id(blob);
	if (id == -1)
		// If not, insert it
		id = q_insert(blob);

	m_cache.insert(make_pair(id, item));
	m_id_cache.insert(make_pair(item, id));
	return id;
}


Attrs::Attrs(utils::sqlite::SQLiteDB& db, const std::set<types::Code>& attrs)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = attrs.begin();
			i != attrs.end(); ++i)
	{
		if (*i == types::TYPE_REFTIME) continue;
		m_attrs.push_back(new AttrSubIndex(db, *i));
	}
}

Attrs::~Attrs()
{
	for (std::vector<AttrSubIndex*>::iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
		delete *i;
}

void Attrs::initDB()
{
	for (std::vector<AttrSubIndex*>::iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
		(*i)->initDB();
}

std::vector<int> Attrs::obtainIDs(const Metadata& md) const
{
	vector<int> ids;
	ids.reserve(m_attrs.size());
	for (const_iterator i = begin(); i != end(); ++i)
		ids.push_back((*i)->insert(md));
	return ids;
}

}
}
}
// vim:set ts=4 sw=4:
