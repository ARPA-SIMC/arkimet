#ifndef ARKI_DATASET_ISEG_INDEX_H
#define ARKI_DATASET_ISEG_INDEX_H

/// Index for the contents of an iseg data file

#include <arki/core/transaction.h>
#include <arki/utils/sqlite.h>
#include <arki/types/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/segment/index/iseg/attr.h>
#include <arki/segment/index/iseg/aggregate.h>
#include <arki/dataset/iseg.h>
#include <string>
#include <set>

struct sqlite3;

namespace arki {
namespace dataset {
class Lock;
class AppendLock;
class CheckLock;

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
class Index
{
public:
    std::shared_ptr<iseg::Dataset> dataset;

protected:
    mutable utils::sqlite::SQLiteDB m_db;

    /// Relative pathname of the segment from the root of the dataset
    std::filesystem::path data_relpath;

    /// Absolute pathname of the data file that we index
    std::filesystem::path data_pathname;

    /// Absolute pathname of the index file
    std::filesystem::path index_pathname;

    // Subtables
    segment::index::iseg::Aggregate* m_uniques = nullptr;
    segment::index::iseg::Aggregate* m_others = nullptr;

    /// Optionally held read lock
    std::shared_ptr<dataset::Lock> lock;

    /// Run PRAGMA calls to setup database behaviour
    void setup_pragmas();

    /// Get a list of all other attribute tables available in the database
    std::set<types::Code> available_other_tables() const;

    /// Initialize m_others
    void init_others();

    /// Get a list of all other attribute tables that can be created in the database
    std::set<types::Code> all_other_tables() const;

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
    void build_md(utils::sqlite::Query& q, Metadata& md, std::shared_ptr<arki::segment::data::Reader> reader) const;

    Index(std::shared_ptr<iseg::Dataset> config, const std::filesystem::path& data_relpath, std::shared_ptr<dataset::Lock> lock=nullptr);

public:
    ~Index();

    bool has_uniques() const { return m_uniques != nullptr; }
    segment::index::iseg::Aggregate& uniques() { return *m_uniques; }
    bool has_others() const { return m_others != nullptr; }
    segment::index::iseg::Aggregate& others() { return *m_others; }

    utils::sqlite::SQLiteDB& db() { return m_db; }

    /**
     * Set of metadata types that make a metadata unique
     */
    std::set<types::Code> unique_codes() const;

    /// Begin a transaction and return the corresponding Pending object
    core::Pending begin_transaction();

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
    bool query_data(const dataset::DataQuery& q, dataset::Session& session, metadata_dest_func dest);

    /**
     * Query this index, returning a summary
     *
     * Summary caches are not used, and the database is always queried.
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    bool query_summary_from_db(const Matcher& m, Summary& summary) const;

    /**
     * Get the metadata for this segment
     */
    void query_segment(metadata_dest_func) const;
};

class RIndex : public Index
{
public:
    RIndex(std::shared_ptr<iseg::Dataset> dataset, const std::filesystem::path& data_relpath, std::shared_ptr<dataset::ReadLock> lock=nullptr);
};

class WIndex : public Index
{
protected:
    utils::sqlite::PrecompiledQuery m_get_current;
    utils::sqlite::InsertQuery m_insert;
    utils::sqlite::PrecompiledQuery m_replace;

    /// Create the tables in the database
    void init_db();

    void compile_insert();

    WIndex(std::shared_ptr<iseg::Dataset> dataset, const std::filesystem::path& data_relpath, std::shared_ptr<dataset::Lock> lock=nullptr);
public:

    /**
     * Index the given metadata item.
     *
     * If the item already exists, returns the blob source pointing to the data
     * currently in the dataset
     */
    std::unique_ptr<types::source::Blob> index(const Metadata& md, uint64_t ofs);

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

    /**
     * Update the index so that the offset of all data in the segment starting
     * from the one at position `data_idx` are shifted backwards by one, so
     * that the ones at `data_idx - 1` and at `data_idx` overlap.
     */
    void test_make_overlap(unsigned overlap_size, unsigned data_idx);

    /**
     * Update the index so that the offset of all data in the segment starting
     * from the one at position `data_idx` are shifted forwards by one, so that
     * a hole is created between the ones at `data_idx - 1` and at `data_idx`
     */
    void test_make_hole(unsigned hole_size, unsigned data_idx);
};


class AIndex : public WIndex
{
public:
    AIndex(std::shared_ptr<iseg::Dataset> config, std::shared_ptr<segment::data::Writer> segment, std::shared_ptr<dataset::AppendLock> lock);
};


class CIndex : public WIndex
{
public:
    CIndex(std::shared_ptr<iseg::Dataset> config, const std::filesystem::path& data_relpath, std::shared_ptr<dataset::CheckLock> lock);
};

}
}
}
#endif
