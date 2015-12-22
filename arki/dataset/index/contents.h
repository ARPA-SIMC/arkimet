#ifndef ARKI_DATASET_INDEX_CONTENTS_H
#define ARKI_DATASET_INDEX_CONTENTS_H

/// Index for data files and their contents

#include <arki/dataset/index.h>
#include <arki/transaction.h>
#include <arki/utils/sqlite.h>
#include <arki/dataset/data.h>
#include <arki/dataset/index/attr.h>
#include <arki/dataset/index/aggregate.h>
#include <arki/dataset/index/summarycache.h>
#include <string>
#include <set>
#include <map>

struct sqlite3;

namespace arki {
class Metadata;
class Matcher;
class ConfigFile;

namespace dataset {
struct DataQuery;

namespace index {

struct Uniques;
struct Others;

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
class Contents : public dataset::Index
{
protected:
    /// Dataset name
    std::string m_name;
    /// Absolute path to the root directory of the dataset
    std::string m_root;
    /// Absolute path of the index database
    std::string m_pathname;

	mutable utils::sqlite::SQLiteDB m_db;
	mutable utils::sqlite::PrecompiledQuery m_get_id;
	mutable utils::sqlite::PrecompiledQuery m_get_current;

	// Subtables
	Aggregate* m_uniques;
	Aggregate* m_others;

    // True if small files are stored in the index
    bool m_smallfiles;

	std::set<types::Code> m_components_indexed;

    mutable SummaryCache scache;

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

	/// Get a list of all other attribute tables available in the database
	std::set<types::Code> available_other_tables() const;

	/// Get a list of all other attribute tables that can be created in the database
	std::set<types::Code> all_other_tables() const;

    /**
     * Rebuild the metadata from the rows in an index query.
     *
     * The rows should be:
     * m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime[, uniq][, other]
     */
    void build_md(utils::sqlite::Query& q, Metadata& md) const;

    Contents(const ConfigFile& cfg);
public:
    ~Contents();

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

    /**
     * Return the metadata for the version of \a md that is already in the
     * database
     *
     * @returns
     *   true if the element was found, else false and \a current is untouched
     */
    bool get_current(const Metadata& md, Metadata& current) const;

	/// Return the number of items currently indexed by this index
	size_t count() const;

    /**
     * Scan all file info in the database, sorted by file and offset
     */
    void scan_files(data::contents_func v) const;

    /**
     * Send the metadata of all data items inside a file to the given consumer
     */
    void scan_file(const std::string& relname, metadata_dest_func consumer, const std::string& orderBy = "offset") const;

    bool segment_timespan(const std::string& relname, types::Time& start_time, types::Time& end_time) const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    bool query_summary(const Matcher& m, Summary& summary) override;

	/**
	 * Query this index, returning a summary
	 *
	 * Summary caches are not used, and the database is always queried.
	 *
	 * @return true if the index could be used for the query, false if the
	 * query does not use the index and a full scan should be used instead
	 */
	bool querySummaryFromDB(const Matcher& m, Summary& summary) const;

	/**
	 * Query summary information from DB using the given WHERE body
	 */
	void querySummaryFromDB(const std::string& where, Summary& summary) const;

    size_t produce_nth(metadata_dest_func consumer, size_t idx) override;

	/**
	 * Run a consistency check on the summary cache, reporting issues
	 * to \a log
	 *
	 * @return
	 *   true if the summary cache looks ok
	 *   false if problems have been found
	 */
	bool checkSummaryCache(std::ostream& log) const;

	/**
	 * Invalidate and rebuild the entire summary cache
	 */
	void rebuildSummaryCache();

    /**
     * Compute the summary for the given month, and output it to \a
     * summary.
     *
     * Cache the result if possible, so that asking again will be much
     * quicker.
     */
    void summaryForMonth(int year, int month, Summary& out) const;

    /**
     * Compute the summary for all the dataset.
     *
     * Cache the result if possible, so that asking again will be much
     * quicker.
     */
    void summaryForAll(Summary& out) const;
};

class RContents : public Contents
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
	void metadataQuery(const std::string& query, metadata::Eater& consumer) const;

public:
	RContents(const ConfigFile& cfg);
	~RContents();

	/// Initialise access to the index
	void open();
};

class WContents : public Contents
{
protected:
	utils::sqlite::InsertQuery m_insert;
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

	void bindInsertParams(utils::sqlite::Query& q, const Metadata& md, const std::string& file, uint64_t ofs, char* timebuf);

public:
	WContents(const ConfigFile& cfg);
	~WContents();

	/**
	 * Initialise access to the index
	 *
	 * @return true if the index did not exist and has been created, false
	 * if it reused the existing index
	 */
	bool open();

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
	void index(const Metadata& md, const std::string& file, uint64_t ofs, int* id = 0);

	/**
	 * Index the given metadata item, or replace it in the index.
	 *
	 * @retval id
	 *   The id of the metadata in the database
	 */
	void replace(Metadata& md, const std::string& file, uint64_t ofs, int* id = 0);

    /**
     * Remove the given metadata item from the index.
     *
     * The removal will only take place when the commit() method will be called
     * on the Pending object.
     */
    void remove(const std::string& relname, off_t ofs);

	/**
	 * Remove all entries from the index
	 */
	void reset();

	/**
	 * Remove all entries from the index that are related to the given data file
	 */
	void reset(const std::string& datafile);

	/// Tidy up the database and reclaim deleted space
	void vacuum();

    /// Flush the journal contents to the main database
    void flush();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
