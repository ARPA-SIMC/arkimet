#ifndef ARKI_DATASET_INDEX_ATTR_H
#define ARKI_DATASET_INDEX_ATTR_H

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

#include <wibble/exception.h>
#include <arki/utils/sqlite.h>
#include <arki/dataset/index/base.h>
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace arki {
namespace matcher {
class OR;
}

namespace dataset {
namespace index {

class AttrSubIndex
{
public:
	// Name of the metadata we index
	std::string name;
	// Serialisation code of the item type that we index
	types::Code code;

protected:
	// This is just a copy of what is in the main index
	utils::sqlite::SQLiteDB& m_db;

	// Precompiled get id statement
	mutable utils::sqlite::PrecompiledQuery* m_select_id;
	// Return the database ID given a string blob. Returns -1 if not found
	int q_select_id(const std::string& blob) const;

	// Precompiled select one statement
	mutable utils::sqlite::PrecompiledQuery* m_select_one;
    // Runs the Item given an ID. Returns an undefined item if not found
    std::unique_ptr<types::Type> q_select_one(int id) const;

	// Precompiled select all statement
	mutable utils::sqlite::PrecompiledQuery* m_select_all;

	// Precompiled insert statement
	utils::sqlite::PrecompiledQuery* m_insert;
	// Insert the blob in the database and return its new ID
	int q_insert(const std::string& blob);

    /// Add an element to the cache
    void add_to_cache(int id, const types::Type& item) const;
    void add_to_cache(int id, const types::Type& item, const std::string& encoded) const;

	// Parsed item cache
	mutable std::map<int, types::Type*> m_cache;

	// Cache of known IDs
	mutable std::map<std::string, int> m_id_cache;

public:
	AttrSubIndex(utils::sqlite::SQLiteDB& db, types::Code serCode);
	~AttrSubIndex();

	void initDB();
	void initQueries() const {}

	/**
	 * Get the ID of the metadata item handled by this AttrSubIndex.
	 *
	 * @returns -1 if the relevant item is not defined in md
	 *
	 * It can raise NotFound if the metadata item is not in the database at
	 * all.
	 */
	int id(const Metadata& md) const;

	void read(int id, Metadata& md) const;

	std::vector<int> query(const matcher::OR& m) const;

	int insert(const Metadata& md);

private:
    AttrSubIndex(const AttrSubIndex&);
    AttrSubIndex& operator==(const AttrSubIndex&);
};

typedef AttrSubIndex RAttrSubIndex;
typedef AttrSubIndex WAttrSubIndex;

class Attrs
{
protected:
	std::vector<AttrSubIndex*> m_attrs;

public:
	typedef std::vector<AttrSubIndex*>::const_iterator const_iterator;

	const_iterator begin() const { return m_attrs.begin(); }
	const_iterator end() const { return m_attrs.end(); }

	Attrs(utils::sqlite::SQLiteDB& db, const std::set<types::Code>& attrs);
	~Attrs();

	void initDB();

	const size_t size() const { return m_attrs.size(); }

	/**
	 * Obtain the IDs of the metadata items in this metadata that
	 * correspond to the member items of this aggregate, inserting the new
	 * metadata items in the database if they are missing
	 */
	std::vector<int> obtainIDs(const Metadata& md) const;
};

}
}
}

// vim:set ts=4 sw=4:
#endif
