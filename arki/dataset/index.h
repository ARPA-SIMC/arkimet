#ifndef ARKI_DATASET_INDEX_H
#define ARKI_DATASET_INDEX_H

/*
 * dataset/index - Dataset index infrastructure
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/transaction.h>
#include <arki/dataset/index/sqlite.h>
#include <arki/dataset/index/attr.h>
#include <string>
#include <set>
#include <map>

struct sqlite3;

namespace arki {
class Metadata;
class MetadataConsumer;
class Matcher;
class ConfigFile;

namespace dataset {

namespace index {
struct InsertQuery : public Query
{
	InsertQuery(SQLiteDB& db) : Query("insert", db) {}

	// Step, but throw DuplicateInsert in case of duplicates
	bool step();
};
}

/**
 * Generic dataset index interface.
 *
 * Every data inside a dataset has a data identifier (or data ID) that is
 * guaranteed to be unique inside the dataset.
 *
 * Every dataset has a dataset index to speed up retrieval of subsets of
 * informations from it.
 *
 * The dataset index needs to be simple and fast, but does not need to support
 * a complex range of queries.
 *
 * It must be possible to completely regenerate the dataset index by
 * rescanning allthe data stored in the dataset.
 */
class DSIndex
{
protected:
	std::string m_root;
	std::string m_indexpath;
	index::SQLiteDB m_db;
	index::InsertQuery m_insert;
	index::PrecompiledQuery m_fetch_by_id;
	index::PrecompiledQuery m_delete;
	index::PrecompiledQuery m_replace;
	index::IDMaker m_id_maker;
	std::set<types::Code> m_components_indexed;
	index::Committer m_committer;

	// True if we index reftime
	bool m_index_reftime;

	// Subtables
	std::map<types::Code, index::AttrSubIndex*> m_sub;

	/// Create the tables in the database
	void initDB();

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

	/// Run a query and output to a consumer all the metadata that come out
	void metadataQuery(const std::string& query, MetadataConsumer& consumer) const;

	void bindInsertParams(index::Query& q, Metadata& md, const std::string& mdid, const std::string& file, size_t ofs, char* timebuf);

public:
	struct DuplicateInsert : public wibble::exception::Consistency
	{
		DuplicateInsert(const std::string& context) throw () :
			wibble::exception::Consistency(context, "duplicate element") {}
		~DuplicateInsert() throw () {}

		virtual const char* type() const throw () { return "DSIndex::DuplicateInsert"; }
	};

	/// Access an index at the given pathname
	DSIndex(const ConfigFile& cfg);
	~DSIndex();

	/// Initialise access to the index
	void open();

    /// Compute the unique ID of a metadata for this index
	std::string id(const Metadata& md) const;

	/// Begin a transaction and return the corresponding Pending object
	Pending beginTransaction();

	/**
	 * Fetch the on-disk location of the given metadata.
	 *
	 * If the metadata is in the index, fetch filename and offset and return
	 * true.  Else, return false.
	 */
	bool fetch(const Metadata& md, std::string& file, size_t& ofs);

	/**
	 * Query this index
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 */
	bool query(const Matcher& m, MetadataConsumer& consumer) const;

    /**
	 * Index the given metadata item.
	 */
	void index(Metadata& md, const std::string& file, size_t ofs);

	/**
	 * Remove the given metadata item from the index.
	 *
	 * The removal will only take place when the commit() method will be called
	 * on the Pending object.
	 */
	void remove(const std::string& id, std::string& file, size_t& ofs);

    /**
	 * Index the given metadata item, or replace it in the index.
	 */
	void replace(Metadata& md, const std::string& file, size_t ofs);

	/**
	 * Remove all entries from the index
	 */
	void reset();

	/**
	 * Remove all entries from the index that are related to the given data file
	 */
	void reset(const std::string& datafile);
};

}
}

// vim:set ts=4 sw=4:
#endif
