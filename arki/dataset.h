#ifndef ARKI_DATASET_H
#define ARKI_DATASET_H

/// Base interface for arkimet datasets
#include <arki/matcher.h>
#include <arki/core/fwd.h>
#include <arki/core/cfg.h>
#include <arki/core/transaction.h>
#include <arki/metadata/fwd.h>
#include <arki/dataset/fwd.h>
#include <string>
#include <vector>
#include <memory>

namespace arki {
class Summary;

/**
 * Generic dataset interface.
 *
 * Archived data is stored in datasets.
 *
 * A dataset is a group of data with similar properties, like the output of a
 * given forecast model, or images from a given satellite.
 *
 * Every dataset has a dataset name.
 *
 * Every data inside a dataset has a data identifier (or data ID) that is
 * guaranteed to be unique inside the dataset.  One can obtain a unique
 * identifier on a piece of data across all datasets by simply prepending the
 * dataset name to the data identifier.
 *
 * Every dataset has a dataset index to speed up retrieval of subsets of
 * informations from it.
 *
 * The dataset index needs to be simple and fast, but does not need to support
 * a complex range of queries.
 *
 * The data is stored in the dataset without alteration from its original
 * form.
 *
 * It must be possible to completely regenerate the dataset index by
 * rescanning allthe data stored in the dataset.
 *
 * A consequence of the last two points is that a dataset can always be fully
 * regenerated by just reimporting data previously extracted from it.
 *
 * A dataset must be fully independent from other datasets, and not must not hold
 * any information about them.
 *
 * A dataset should maintain a summary of its contents, that lists the
 * various properties of the data it contains.
 *
 * Since the dataset groups data with similar properties, the summary is intended
 * to change rarely.
 *
 * An archive index can therefore be built, indexing all dataset summaries,
 * to allow complex data searches across datasets.
 */
namespace dataset {

/// Interface for generating progress updates on queries
class QueryProgress
{
protected:
    size_t expected_count = 0;
    size_t expected_bytes = 0;
    size_t count = 0;
    size_t bytes = 0;

public:
    virtual ~QueryProgress();

    virtual void start(size_t expected_count=0, size_t expected_bytes=0);
    virtual void update(size_t count, size_t bytes);
    virtual void done();

    /// Wrap a metadata_dest_func to provide updates to this QueryProgress
    metadata_dest_func wrap(metadata_dest_func);
};

struct DataQuery
{
    /// Matcher used to select data
    Matcher matcher;

    /**
     * Hint for the dataset backend to let them know that we also want the data
     * and not just the metadata.
     *
     * This is currently used:
     *  - by the HTTP client dataset, which will only download data from the
     *    server if this option is set
     *  - by local datasets to read-lock the segments for the duration of the
     *    query
     */
    bool with_data;

    /// Optional compare function to define a custom ordering of the result
    std::shared_ptr<metadata::sort::Compare> sorter;

    /// Optional progress reporting
    std::shared_ptr<QueryProgress> progress;

    DataQuery();
    DataQuery(const std::string& matcher, bool with_data=false);
    DataQuery(const Matcher& matcher, bool with_data=false);
    ~DataQuery();
};

struct ByteQuery : public DataQuery
{
    enum Type {
        BQ_DATA = 0,
        BQ_POSTPROCESS = 1,
    };

    std::string param;
    Type type = BQ_DATA;
    std::function<void(core::NamedFileDescriptor&)> data_start_hook = nullptr;

    ByteQuery() {}

    void setData(const Matcher& m)
    {
        with_data = true;
        type = BQ_DATA;
        matcher = m;
    }

    void setPostprocess(const Matcher& m, const std::string& procname)
    {
        with_data = true;
        type = BQ_POSTPROCESS;
        matcher = m;
        param = procname;
    }
};


/// Base dataset configuration
class Dataset : public std::enable_shared_from_this<Dataset>
{
protected:
    std::shared_ptr<Dataset> m_parent;

    /// Dataset name
    std::string m_name;

public:
    /// Work session
    std::shared_ptr<Session> session;

    /// Raw configuration key-value pairs
    core::cfg::Section cfg;

    Dataset(std::shared_ptr<Session> session);
    Dataset(std::shared_ptr<Session> session, const std::string& name);
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
    virtual ~Dataset() {}

    /// Return the dataset name
    std::string name() const;

    virtual std::shared_ptr<Reader> create_reader();
    virtual std::shared_ptr<Writer> create_writer();
    virtual std::shared_ptr<Checker> create_checker();

    /**
     * Set a parent dataset.
     *
     * Datasets can be nested to delegate management of part of the dataset
     * contents to a separate dataset. Hierarchy is tracked so that at least a
     * full dataset name can be computed in error messages.
     */
    void set_parent(std::shared_ptr<Dataset> parent);
};


/**
 * Base class for all dataset Readers, Writers and Checkers.
 */
struct Base
{
public:
    Base() {}
    Base(const Base&) = delete;
    Base(const Base&&) = delete;
    virtual ~Base() {}
    Base& operator=(const Base&) = delete;
    Base& operator=(Base&&) = delete;

    /// Return the dataset
    virtual const Dataset& dataset() const = 0;

    /// Return the dataset
    virtual Dataset& dataset() = 0;

    /// Return the dataset name
    std::string name() const { return dataset().name(); }

    /// Return the dataset configuration
    const core::cfg::Section& cfg() const { return dataset().cfg; }

    /// Return a name identifying the dataset type
    virtual std::string type() const = 0;
};

class Reader : public dataset::Base
{
public:
    using Base::Base;

    /**
     * Query the dataset using the given matcher, and sending the results to
     * the given function.
     *
     * Returns true if dest always returned true, false if iteration stopped
     * because dest returned false.
     */
    virtual bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

    /**
     * Add to summary the summary of the data that would be extracted with the
     * given query.
     */
    virtual void query_summary(const Matcher& matcher, Summary& summary);

    /**
     * Query the dataset obtaining a byte stream, that gets written to a file
     * descriptor.
     *
     * The default implementation in Reader is based on queryData.
     */
    virtual void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out);

    /**
     * Query the dataset obtaining a byte stream, that gets written to a file
     * descriptor.
     *
     * The default implementation in Reader is based on queryData.
     */
    virtual void query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out);

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this dataset.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * dataset.
     */
    virtual void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end);
};

struct WriterBatchElement
{
    /// Metadata to acquire
    Metadata& md;
    /// Name of the dataset where it has been acquired (empty when not
    /// acquired)
    std::string dataset_name;
    /// Acquire result
    WriterAcquireResult result = ACQ_ERROR;

    WriterBatchElement(Metadata& md) : md(md) {}
    WriterBatchElement(const WriterBatchElement& o) = default;
    WriterBatchElement(WriterBatchElement&& o) = default;
    WriterBatchElement& operator=(const WriterBatchElement& o) = default;
    WriterBatchElement& operator=(WriterBatchElement&& o) = default;
};

struct WriterBatch : public std::vector<std::shared_ptr<WriterBatchElement>>
{
    /**
     * Set all elements in the batch to ACQ_ERROR
     */
    void set_all_error(const std::string& note);
};


enum ReplaceStrategy {
    /// Default strategy, as configured in the dataset
    REPLACE_DEFAULT,
    /// Never replace
    REPLACE_NEVER,
    /// Always replace
    REPLACE_ALWAYS,
    /**
     * Replace if update sequence number is higher (do not replace if USN
     * not available)
     */
    REPLACE_HIGHER_USN,
};


struct AcquireConfig
{
    ReplaceStrategy replace=REPLACE_DEFAULT;
    bool drop_cached_data_on_commit=false;

    AcquireConfig() = default;
    AcquireConfig(ReplaceStrategy replace) : replace(replace) {}
    AcquireConfig(const AcquireConfig&) = default;
    AcquireConfig(AcquireConfig&&) = default;
    AcquireConfig& operator=(const AcquireConfig&) = default;
    AcquireConfig& operator=(AcquireConfig&&) = default;
};


class Writer : public dataset::Base
{
public:
    using Base::Base;

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * After acquiring the data successfully, the data can be retrieved from
     * the dataset.  Also, information such as the dataset name and the id of
     * the data in the dataset are added to the Metadata object.
     *
     * @return The outcome of the operation.
     */
    virtual WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) = 0;

    /**
     * Acquire the given metadata items (and related data) in this dataset.
     *
     * After acquiring the data successfully, the data can be retrieved from
     * the dataset.  Also, information such as the dataset name and the id of
     * the data in the dataset are added to the Metadata in the collection
     *
     * @return The outcome of the operation, as a vector with an WriterAcquireResult
     * for each metadata in the collection.
     */
    virtual void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) = 0;

    /**
     * Remove the given metadata from the database.
     */
    virtual void remove(Metadata& md) = 0;

    /**
     * Flush pending changes to disk
     */
    virtual void flush();

    /**
     * Obtain a write lock on the database, held until the Pending is committed
     * or rolled back.
     *
     * This is only used for testing.
     */
    virtual core::Pending test_writelock();

    /**
     * Simulate acquiring the given metadata item (and related data) in this
     * dataset.
     *
     * No change of any kind happens to the dataset.
     */
    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};

struct CheckerConfig
{
    /// Reporter that gets notified of check progress and results
    std::shared_ptr<dataset::Reporter> reporter;
    /// If set, work only on segments that could contain data that matches this
    /// matcher
    Matcher segment_filter;
    /// Work on offline archives
    bool offline = true;
    /// Work on online data
    bool online = true;
    /// Simulate, do not write any changes
    bool readonly = true;
    /// Perform optional and time consuming operations
    bool accurate = false;

    CheckerConfig();
    CheckerConfig(std::shared_ptr<dataset::Reporter> reporter, bool readonly=true);
    CheckerConfig(const CheckerConfig&) = default;
    CheckerConfig(CheckerConfig&&) = default;
    CheckerConfig& operator=(const CheckerConfig&) = default;
    CheckerConfig& operator=(CheckerConfig&&) = default;
};

struct Checker : public dataset::Base
{
    using Base::Base;

    /**
     * Repack the dataset.
     *
     * test_flags are used to select alternate and usually undesirable repack
     * behaviours during tests, and should always be 0 outside tests.
     */
    virtual void repack(CheckerConfig& opts, unsigned test_flags=0) = 0;

    /// Check the dataset for errors
    virtual void check(CheckerConfig& opts) = 0;

    /// Remove data from the dataset that is older than `delete age`
    virtual void remove_old(CheckerConfig& opts) = 0;

    /// Remove all data from the dataset
    virtual void remove_all(CheckerConfig& opts) = 0;

    /// Convert directory segments into tar segments
    virtual void tar(CheckerConfig& opts) = 0;

    /// Convert directory segments into zip segments
    virtual void zip(CheckerConfig& opts) = 0;

    /// Convert directory segments into .gz/.gz.idx segments
    virtual void compress(CheckerConfig& opts, unsigned groupsize) = 0;

    /// Send the dataset state to the reporter
    virtual void state(CheckerConfig& opts) = 0;

    /**
     * Check consistency of the last byte of GRIB and BUFR data in the archive,
     * optionally fixing it.
     *
     * See https://github.com/ARPAE-SIMC/arkimet/issues/51 for details.
     */
    virtual void check_issue51(CheckerConfig& opts);
};

}
}
#endif
