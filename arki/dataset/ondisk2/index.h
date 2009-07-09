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
#include <arki/dataset/ondisk2/aggregate.h>
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

struct Uniques;
struct Others;

namespace writer {
class IndexFileVisitor;
}

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

	mutable utils::sqlite::SQLiteDB m_db;
	mutable utils::sqlite::PrecompiledQuery m_fetch_by_id;
	mutable utils::sqlite::PrecompiledQuery m_get_id;

	// Subtables
	Aggregate* m_uniques;
	Aggregate* m_others;

	std::set<types::Code> m_components_indexed;

	/**
	 * Add to 'query' the SQL joins and constraints based on the given matcher.
	 *
	 * An example string that can be added is:
	 *  "JOIN mduniq AS u ON uniq = u.id WHERE reftime = (...) AND u.origin IN (1, 2, 3)"
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 *
	 * It can raise dataset::index::NotFound if some parts of m do not
	 * match any metadata in the database.
	 */
	bool addJoinsAndConstraints(const Matcher& m, std::string& query) const;

	Index(const ConfigFile& cfg);
public:
	~Index();

	const std::string& pathname() const { return m_pathname; }

	inline bool is_indexed(types::Code c) const
	{
		return m_components_indexed.find(c) != m_components_indexed.end();
	}

	/**
	 * Set of metadata types that make a metadata unique
	 */
	std::set<types::Code> unique_codes() const;

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

	/// Run PRAGMA calls to setup database behaviour
	void setupPragmas();

	/// Return the database ID of a metadata in this index.  If the
	/// metadata is not there, return -1.
	int id(const Metadata& md) const;

	/// Return the number of items currently indexed by this index
	size_t count() const;

	/**
	 * Scan all file info in the database, sorted by file and offset
	 */
	void scan_files(writer::IndexFileVisitor& v, const std::string& orderBy = "file, offset") const;

	/**
	 * Scan the information about the given file, sorted by offset
	 */
	void scan_file(const std::string& relname, writer::IndexFileVisitor& v, const std::string& orderBy = "offset") const;

	/**
	 * Return the maximum reference time found in the given file.
	 *
	 * Return the empty string if the file is not in the index.
	 */
	std::string max_file_reftime(const std::string& relname) const;

	/**
	 * Query this index, returning metadata
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 */
	bool query(const Matcher& m, MetadataConsumer& consumer) const;

	/**
	 * Query this index, returning a summary
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 */
	bool querySummary(const Matcher& m, Summary& summary) const;
};

class RIndex : public Index
{
protected:
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
};

class WIndex : public Index
{
protected:
	index::InsertQuery m_insert;
	utils::sqlite::PrecompiledQuery m_delete;
	utils::sqlite::PrecompiledQuery m_replace;

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

	/// Begin an EXCLUSIVE transaction and return the corresponding Pending object
	Pending beginExclusiveTransaction();

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

	/**
	 * Update the offset of the piece of data 'id' to newofs inside
	 * the same file
	 */
	void relocate_data(int id, off_t newofs);

	/// Tidy up the database and reclaim deleted space
	void vacuum();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
