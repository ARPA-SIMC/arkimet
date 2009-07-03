/*
 * dataset/ondisk2/aggregate - Handle aggregate tables in the index
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

#include <arki/dataset/ondisk2/aggregate.h>
#include <arki/dataset/index/base.h>
#include <arki/matcher.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace ondisk2 {

void Aggregate::initQueries()
{
	string query = "SELECT id FROM " + m_table_name + " WHERE " ;
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		if (i != m_attrs.begin())
			query += " AND ";
		query += (*i)->name + "=?";
	}
	q_select.compile(query);

	query.clear();
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		if (!query.empty()) query += ", ";
		query += (*i)->name;
	}

	q_select_by_id.compile("SELECT " + query + " FROM " + m_table_name + " WHERE id=?");

	string names;
	string placeholders;
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		if (not names.empty())
		{
			names += ", ";
			placeholders += ", ";
		}
		names += (*i)->name;
		placeholders += "?";
	}
	q_insert.compile("INSERT INTO " + m_table_name + " (" + names + ") VALUES (" + placeholders + ")");
}

std::set<types::Code> Aggregate::members() const
{
	std::set<types::Code> res;
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
		res.insert((*i)->code);
	return res;
}

int Aggregate::get(const Metadata& md) const
{
	q_select.reset();
	int idx = 0;
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		int id = -1;
		try {
			id = (*i)->id(md);
		} catch (index::NotFound) {
			return -1;
		}

		// If this metadata item does not exist, then the aggregate
		// also does not exists
		/*
		if (id == -1)
			q_select.bindNull(++idx);
		else
		*/
		q_select.bind(++idx, id);
	}

	int id = -1;
	while (q_select.step())
		id = q_select.fetchInt(0);
	return id;
}

void Aggregate::read(int id, Metadata& md) const
{
	q_select_by_id.reset();
	q_select_by_id.bind(1, id);
	while (q_select_by_id.step())
	{
		size_t idx = 0;
		for (index::Attrs::const_iterator i = m_attrs.begin();
				i != m_attrs.end(); ++i, ++idx)
			//if (q_select_by_id.fetchType(idx) != SQLITE_NULL)
		{
			int id = q_select_by_id.fetchInt(idx);
			if (id != -1)
				(*i)->read(id, md);
		}
	}
}

int Aggregate::add_constraints(
		const Matcher& m,
		std::vector<std::string>& constraints,
		const std::string& prefix) const
{
	if (m.empty()) return 0;

	// See if the matcher has anything that we can use
	size_t found = 0;
	for (index::Attrs::const_iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
	{
		matcher::AND::const_iterator mi = m.m_impl->find((*i)->code);
		if (mi == m.m_impl->end())
			continue;

		// We found something: generate a constraint for it
		constraints.push_back(
				prefix + "." + (*i)->name + " " +
				index::fmtin((*i)->query(*(mi->second->upcast<matcher::OR>()))));
		++found;
	}
	return found;
}


int Aggregate::obtain(const Metadata& md)
{
	// First check if we have it
	vector<int> ids = m_attrs.obtainIDs(md);
	q_select.reset();
	for (size_t i = 0; i < ids.size(); ++i)
		/*
		if (ids[i] == -1)
			q_select.bindNull(i + 1);
		else
		*/
		q_select.bind(i + 1, ids[i]);

	int id = -1;
	while (q_select.step())
		id = q_select.fetchInt(0);

	if (id != -1)
		return id;

	// If we do not have it, create it
	q_insert.reset();
	for (size_t i = 0; i < ids.size(); ++i)
		/*
		if (ids[i] == -1)
			q_insert.bindNull(i + 1);
		else
		*/
		q_insert.bind(i + 1, ids[i]);

	q_insert.step();

	return m_db.lastInsertID();
}

void Aggregate::initDB(const std::set<types::Code>& components_indexed)
{
	m_attrs.initDB();

	// Table holding the metadata combinations that are unique in a certain
	// istant of time
	string query = "CREATE TABLE IF NOT EXISTS " + m_table_name + " ("
		"id INTEGER PRIMARY KEY";
	string unique = ", UNIQUE(";
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		query += ", " + (*i)->name + " INTEGER NOT NULL";
		if (i != m_attrs.begin())
			unique += ", ";
		unique += (*i)->name;
	}
	query += unique + ") )";
	m_db.exec(query);

	// Create indices on mduniq if needed
	for (index::Attrs::const_iterator i = m_attrs.begin();
			i != m_attrs.end(); ++i)
	{
		if (components_indexed.find((*i)->code) == components_indexed.end()) continue;
		string name = (*i)->name;
		m_db.exec("CREATE INDEX IF NOT EXISTS " + m_table_name + "_idx_" + name
							+ " ON " + m_table_name + " (" + name + ")");
	}
}

}
}
}
