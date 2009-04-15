#ifndef ARKI_DATASET_ONDISK2_INDEX_H
#define ARKI_DATASET_ONDISK2_INDEX_H

/*
 * dataset/ondisk2/index - Dataset index infrastructure
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
namespace ondisk2 {

/**
 * Dataset index.
 *
 * Every indexed metadata item that can be summarised has a table (attr table)
 * that maps its binary representation to an ID.
 *
 * Every indexed metadata is a row that contains all non-summarisable metadata
 * items, plus the attr table IDs of all summarisable items.
 *
 * Summaries can be generated on the fly from the index with an aggregate
 * query.
 *
 * Uniqueness is enforced with UNIQUE constraints.
 *
 * It must be possible to completely regenerate the dataset index by
 * rescanning all the data stored in the dataset.
 */
class Index
{
protected:
	std::string m_name;
	std::string m_root;
	std::string m_indexpath;
	std::string m_pathname;

	std::set<types::Code> m_components_unique;
	std::set<types::Code> m_components_indexed;

	Index(const ConfigFile& cfg);
public:
	~Index();
};

class RIndex : public Index
{
protected:
	utils::sqlite::SQLiteDB m_db;
	mutable utils::sqlite::PrecompiledQuery m_get_id;
	mutable utils::sqlite::PrecompiledQuery m_fetch_by_id;

	// Subtables
	std::map<types::Code, index::RAttrSubIndex*> m_rsub;

	/// Run PRAGMA calls to setup database behaviour
	void setupPragmas();

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

	/// Run a query and output to a consumer all the metadata that come out
	void metadataQuery(const std::string& query, MetadataConsumer& consumer) const;

	/// Run a query and output to a summary all the metadata that come out
	void summaryQuery(const std::string& query, Summary& summary) const;

public:
	RIndex(const ConfigFile& cfg);
	~RIndex();

	/// Initialise access to the index
	void open();

	/// Return the database ID of a metadata in this index.  If the
	/// metadata is not there, return -1.
	int id(const Metadata& md) const;

#if 0
	/**
	 * Fetch the on-disk location of the given metadata.
	 *
	 * If the metadata is in the index, fetch filename and offset and return
	 * true.  Else, return false.
	 */
	bool fetch(const Metadata& md, std::string& file, size_t& ofs);
#endif

	/**
	 * Query this index
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 */
	bool query(const Matcher& m, MetadataConsumer& consumer) const;

	bool querySummary(const Matcher& m, Summary& summary) const;
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

	void bindInsertParams(utils::sqlite::Query& q, Metadata& md, const std::string& file, size_t ofs, char* timebuf);

public:
	WIndex(const ConfigFile& cfg);
	~WIndex();

	/// Initialise access to the index
	void open();

	/// Begin a transaction and return the corresponding Pending object
	Pending beginTransaction();

	/**
	 * Index the given metadata item.
	 *
	 * @retval id
	 *   The id of the metadata in the database
	 */
	void index(Metadata& md, const std::string& file, size_t ofs, int* id = 0);

	/**
	 * Index the given metadata item, or replace it in the index.
	 *
	 * @retval id
	 *   The id of the metadata in the database
	 */
	void replace(Metadata& md, const std::string& file, size_t ofs, int* id = 0);

	/**
	 * Remove the given metadata item from the index.
	 *
	 * The removal will only take place when the commit() method will be called
	 * on the Pending object.
	 */
	void remove(int id, std::string& file);

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
}

// vim:set ts=4 sw=4:
#endif
