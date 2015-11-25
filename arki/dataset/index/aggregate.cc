#include "config.h"
#include "base.h"
#include "aggregate.h"
#include "arki/matcher.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace index {

void Aggregate::initQueries()
{
	string query = "SELECT id FROM " + m_table_name + " WHERE " ;
    for (Attrs::const_iterator i = m_attrs.begin();
            i != m_attrs.end(); ++i)
    {
		if (i != m_attrs.begin())
			query += " AND ";
		query += (*i)->name + "=?";
	}
	q_select.compile(query);

    query.clear();
    for (Attrs::const_iterator i = m_attrs.begin();
            i != m_attrs.end(); ++i)
    {
		if (!query.empty()) query += ", ";
		query += (*i)->name;
	}

	q_select_by_id.compile("SELECT " + query + " FROM " + m_table_name + " WHERE id=?");

	string names;
	string placeholders;
    for (Attrs::const_iterator i = m_attrs.begin();
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
    for (Attrs::const_iterator i = m_attrs.begin();
            i != m_attrs.end(); ++i)
        res.insert((*i)->code);
    return res;
}

int Aggregate::get(const Metadata& md) const
{
	q_select.reset();
	int idx = 0;
    for (Attrs::const_iterator i = m_attrs.begin();
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
		id = q_select.fetch<int>(0);
	return id;
}

void Aggregate::read(int id, Metadata& md) const
{
	std::map< int, std::vector<int> >::iterator i = m_cache.find(id);
	if (i == m_cache.end())
	{
		vector<int> vals;
		q_select_by_id.reset();
		q_select_by_id.bind(1, id);
		while (q_select_by_id.step())
			for (size_t idx = 0; idx < m_attrs.size(); ++idx)
				vals.push_back(q_select_by_id.fetch<int>(idx));
		pair<std::map< int, std::vector<int> >::iterator, bool> ins =
			m_cache.insert(make_pair(id, vals));
		i = ins.first;
	}

    size_t idx = 0;
    for (Attrs::const_iterator j = m_attrs.begin();
            j != m_attrs.end(); ++j, ++idx)
	{
		int id = i->second[idx];
		if (id != -1)
			(*j)->read(id, md);
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
    for (Attrs::const_iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
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

std::string Aggregate::make_subquery(const Matcher& m) const
{
	if (m.empty()) return std::string();

    // See if the matcher has anything that we can use
    vector<string> constraints;
    for (Attrs::const_iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
    {
		matcher::AND::const_iterator mi = m.m_impl->find((*i)->code);
		if (mi == m.m_impl->end())
			continue;

		// We found something: generate a constraint for it
		constraints.push_back(
				(*i)->name + " " +
				index::fmtin((*i)->query(*(mi->second->upcast<matcher::OR>()))));
	}
	if (constraints.empty())
		return std::string();
	return "SELECT id FROM " + m_table_name + " WHERE " + str::join(" AND ", constraints.begin(), constraints.end());
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
		id = q_select.fetch<int>(0);

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
    for (Attrs::const_iterator i = m_attrs.begin();
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
    for (Attrs::const_iterator i = m_attrs.begin();
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
