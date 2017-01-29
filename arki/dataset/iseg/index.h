#ifndef ARKI_DATASET_ISEG_INDEX_H
#define ARKI_DATASET_ISEG_INDEX_H

/// Index for the contents of an iseg data file

#include <arki/transaction.h>
#include <arki/utils/sqlite.h>
#include <arki/dataset/segment.h>
#include <arki/dataset/iseg.h>
#include <arki/dataset/index/attr.h>
#include <arki/dataset/index/aggregate.h>
//#include <arki/dataset/index/summarycache.h>
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
}

namespace iseg {

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
class Index : public Segment::Payload
{
protected:
    std::shared_ptr<const iseg::Config> m_config;
    mutable utils::sqlite::SQLiteDB m_db;

    /// Relative pathname of the segment from the root of the dataset
    std::string data_relpath;

    /// Absolute pathname of the data file that we index
    std::string data_pathname;

    /// Absolute pathname of the index file
    std::string index_pathname;

    // Subtables
    index::Aggregate* m_uniques = nullptr;
    index::Aggregate* m_others = nullptr;

    /// Run PRAGMA calls to setup database behaviour
    void setup_pragmas();

    /// Get a list of all other attribute tables available in the database
    std::set<types::Code> available_other_tables() const;

    /// Initialize m_others
    void init_others();

    /// Get a list of all other attribute tables that can be created in the database
    std::set<types::Code> all_other_tables() const;

#if 0
    mutable utils::sqlite::PrecompiledQuery m_get_id;
    mutable utils::sqlite::PrecompiledQuery m_get_current;
#endif

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
    void add_joins_and_constraints(const Matcher& m, std::string& query) const;

    /**
     * Rebuild the metadata from the rows in an index query.
     *
     * The rows should be:
     * m.offset, m.size, m.notes, m.reftime[, uniq][, other]
     */
    void build_md(utils::sqlite::Query& q, Metadata& md) const;

    Index(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath);

public:
    ~Index();

    const iseg::Config& config() const { return *m_config; }

    /**
     * Set of metadata types that make a metadata unique
     */
    std::set<types::Code> unique_codes() const;

#if 0
    const std::string& pathname() const { return config().index_pathname; }

	inline bool is_indexed(types::Code c) const
	{
		return m_components_indexed.find(c) != m_components_indexed.end();
	}

	/**
	 * Precompile queries.
	 *
	 * This must be called after the database schema has been created, as a
	 * change in the database schema invalidates precompiled queries.
	 */
	void initQueries();

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

    bool segment_timespan(const std::string& relname, core::Time& start_time, core::Time& end_time) const override;
#endif

    /**
     * Send the metadata of all data items known by the index
     */
    void scan(metadata_dest_func consumer, const std::string& order_by="offset") const;

    /**
     * Query data sending the results to the given consumer.
     *
     * Returns true if dest returned true all the time, false if generation
     * stopped because dest returned false.
     */
    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest);
#if 0
    bool query_summary(const Matcher& m, Summary& summary) override;
#endif

    /**
     * Query this index, returning a summary
     *
     * Summary caches are not used, and the database is always queried.
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    bool query_summary_from_db(const Matcher& m, Summary& summary) const;

#if 0
	/**
	 * Query summary information from DB using the given WHERE body
	 */
	void querySummaryFromDB(const std::string& where, Summary& summary) const;

    /**
     * Run a consistency check on the summary cache, reporting issues
     * to \a log
     *
     * @return
     *   true if the summary cache looks ok
     *   false if problems have been found
     */
    bool checkSummaryCache(const dataset::Base& ds, Reporter& reporter) const;

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
#endif
};

class RIndex : public Index
{
public:
    RIndex(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath);
};

class WIndex : public Index
{
protected:
    utils::sqlite::InsertQuery m_insert;
    utils::sqlite::PrecompiledQuery m_replace;

    /// Create the tables in the database
    void init_db();

    void compile_insert();
    void bind_insert(utils::sqlite::Query& q, const Metadata& md, uint64_t ofs, char* timebuf);

public:
    WIndex(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath);
#if 0
    ~WContents();

	/**
	 * Initialise access to the index
	 *
	 * @return true if the index did not exist and has been created, false
	 * if it reused the existing index
	 */
	bool open();
#endif

    /// Begin a transaction and return the corresponding Pending object
    Pending begin_transaction();

    /// Begin an EXCLUSIVE transaction and return the corresponding Pending object
    Pending begin_exclusive_transaction();

    /**
     * Index the given metadata item.
     */
    void index(const Metadata& md, uint64_t ofs);

    /**
     * Index the given metadata item, or replace it in the index.
     */
    void replace(Metadata& md, uint64_t ofs);

    /**
     * Remove the given metadata item from the index.
     */
    void remove(off_t ofs);

    /// Flush the journal contents to the main database
    void flush();

    /**
     * Remove all entries from the index
     */
    void reset();

    /// Tidy up the database and reclaim deleted space
    void vacuum();
};

}
}
}
#endif
