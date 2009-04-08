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
#include <arki/transaction.h>
#include <arki/utils/sqlite.h>
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
struct InsertQuery : public utils::sqlite::Query
{
	InsertQuery(utils::sqlite::SQLiteDB& db) : utils::sqlite::Query("insert", db) {}

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
class Index
{
protected:
	std::string m_root;
	std::string m_indexpath;
	std::string m_pathname;

	index::IDMaker m_id_maker;
	std::set<types::Code> m_components_indexed;

	// True if we index reftime
	bool m_index_reftime;

	Index(const ConfigFile& cfg);

public:
	struct DuplicateInsert : public wibble::exception::Consistency
	{
		DuplicateInsert(const std::string& context) throw () :
			wibble::exception::Consistency(context, "duplicate element") {}
		~DuplicateInsert() throw () {}

		virtual const char* type() const throw () { return "DSIndex::DuplicateInsert"; }
	};

	~Index();
};

class RIndex : public Index
{
protected:
	utils::sqlite::SQLiteDB m_db;
	utils::sqlite::PrecompiledQuery m_fetch_by_id;

	// Subtables
	std::map<types::Code, index::RAttrSubIndex*> m_rsub;

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

	/// Run a query and output to a consumer all the metadata that come out
	void metadataQuery(const std::string& query, MetadataConsumer& consumer) const;

public:
	RIndex(const ConfigFile& cfg);
	~RIndex();

	/// Initialise access to the index
	void open();

	/// Compute the unique ID of a metadata for this index
	std::string id(const Metadata& md) const;

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
};

class WIndex : public RIndex
{
protected:
	index::InsertQuery m_insert;
	utils::sqlite::PrecompiledQuery m_delete;
	utils::sqlite::PrecompiledQuery m_replace;
	utils::sqlite::Committer m_committer;

	// Subtables
	std::map<types::Code, index::WAttrSubIndex*> m_wsub;

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

	/// Create the tables in the database
	void initDB();

	void bindInsertParams(utils::sqlite::Query& q, Metadata& md, const std::string& mdid, const std::string& file, size_t ofs, char* timebuf);

public:
	WIndex(const ConfigFile& cfg);
	~WIndex();

	/// Initialise access to the index
	void open();

	/// Begin a transaction and return the corresponding Pending object
	Pending beginTransaction();

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
