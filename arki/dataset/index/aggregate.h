#ifndef ARKI_DATASET_INDEX_AGGREGATE_H
#define ARKI_DATASET_INDEX_AGGREGATE_H

/*
 * dataset/index/aggregate - Handle aggregate tables in SQL indices
 *
 * Copyright (C) 2009--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/sqlite.h>
#include <arki/dataset/index/attr.h>

#include <vector>
#include <set>
#include <string>

namespace arki {
namespace dataset {
namespace index {

class Aggregate
{
protected:
	utils::sqlite::SQLiteDB& m_db;
	std::string m_table_name;
    Attrs m_attrs;
	mutable std::map< int, std::vector<int> > m_cache;

	mutable utils::sqlite::PrecompiledQuery q_select;
	mutable utils::sqlite::PrecompiledQuery q_select_by_id;
	mutable utils::sqlite::PrecompiledQuery q_insert;

    void init_select() const;
    void init_select_by_id() const;
    void init_insert() const;

public:
	Aggregate(utils::sqlite::SQLiteDB& db,
		  const std::string& table_name,
		  const std::set<types::Code>& members)
		: m_db(db), m_table_name(table_name), m_attrs(db, members),
		  q_select("sel", db),
		  q_select_by_id("selbyid", db),
		  q_insert("ins", db) {}
	~Aggregate() {}

	/**
	 * Get the set of types::Code handled by this aggregate
	 */
	std::set<types::Code> members() const;

	/**
	 * Get the ID of the row that matches the given metadata.
	 *
	 * @returns the ID, or -1 if it was not found.
	 */
	int get(const Metadata& md) const;

	/**
	 * Read the metadata fields corresponding to the given ID from the
	 * database and store them into the metadata
	 */
	void read(int id, Metadata& md) const;

	/**
	 * Add to a vector of strings representing SQL WHERE constraints, the
	 * constraints on this table that can be computed from the relevant
	 * parts of the given matcher.
	 *
	 * @param prefix
	 *   The prefix to add to the table name, useful in JOINs
	 * @returns
	 *   The number of constraints added to the vector
	 */
	int add_constraints(
		const Matcher& m,
		std::vector<std::string>& constraints,
		const std::string& prefix) const;

	/**
	 * Create a subquery selecting the IDs corresponding to the given
	 * matcher
	 */
	std::string make_subquery(const Matcher& m) const;

	/**
	 * Obtain the aggregate ID for this metadata, inserting the new
	 * aggregate item in the database if it is missing
	 */
	int obtain(const Metadata& md);

	void initDB(const std::set<types::Code>& components_indexed);
};

}
}
}

#endif
