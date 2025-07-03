#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/segment/data.h>
#include <arki/core/time.h>

namespace arki::dataset::segmented {
struct TestHooks;

struct Dataset : public local::Dataset
{
protected:
    /// dataset::Step for this configuration
    std::shared_ptr<Step> m_step;

    Dataset(const Dataset& cfg) = default;

public:
    /// Segment session used to instantiate segment accessors
    std::shared_ptr<segment::Session> segment_session;

    /// Name of the dataset::Step used to dispatch data into segments
    std::string step_name;

    /// What replace strategy to use when acquire() is called with REPLACE_DEFAULT
    ReplaceStrategy default_replace_strategy;

    /**
     * If true, this dataset is used as an archive for offline data
     */
    bool offline = false;

    /**
     * If true, try to store the content of small files in the index if
     * possible, to avoid extra I/O when querying
     */
    bool smallfiles = false;

    /**
     * Maximum number of data items compressed together in gzipped segments.
     *
     * Use 0 to disable grouping and indexing of compressed segments.
     */
    unsigned gz_group_size = 512;

    /**
     * Trade write reliability and write concurrency in favour of performance.
     *
     * Useful for writing fast to temporary private datasets.
     */
    bool eatmydata = false;

    /**
     * Set in test suite to set hooks in various points of dataset processing
     */
    std::shared_ptr<TestHooks> test_hooks;


    Dataset(std::shared_ptr<Session> session, std::shared_ptr<segment::Session> segment_session, const core::cfg::Section& cfg);
    ~Dataset();

    std::shared_ptr<archive::Dataset> archive() override;

    virtual bool relpath_timespan(const std::filesystem::path& path, core::Interval& interval) const;

    const Step& step() const { return *m_step; }
};

/**
 * LocalReader dataset with data stored in segment files
 */
class Reader : public local::Reader
{
public:
    using local::Reader::Reader;
    ~Reader();

    const Dataset& dataset() const override = 0;
    Dataset& dataset() override = 0;
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class Writer : public local::Writer
{
protected:
    std::map<std::string, metadata::InboundBatch> batch_by_segment(metadata::InboundBatch& batch);

public:
    using local::Writer::Writer;
    ~Writer();

    const Dataset& dataset() const override = 0;
    Dataset& dataset() override = 0;

    // TODO: make it a member and refactor TestDispatcher to instantiate dataset writers
    static void test_acquire(std::shared_ptr<Session>, const core::cfg::Section& cfg, metadata::InboundBatch& batch);
};


/**
 * State of a segment in the dataset
 */
struct SegmentState
{
    /// Segment state
    arki::segment::State state;
    /// Allowed time interval for data in the segment
    core::Interval interval;

    SegmentState() : state(segment::SEGMENT_OK) {}
    explicit SegmentState(arki::segment::State state)
        : state(state) {}
    SegmentState(arki::segment::State state, const core::Interval& interval)
        : state(state), interval(interval) {}
    SegmentState(const SegmentState&) = default;
    SegmentState(SegmentState&&) = default;

    /// Check if this segment is old enough to be deleted or archived
    void check_age(const std::filesystem::path& relpath, const Dataset& cfg, dataset::Reporter& reporter);
};


class CheckerSegment
{
protected:
    // Allow subclasses to hook processing while the Fixer lock is still held
    virtual void post_repack(std::shared_ptr<segment::Fixer>, segment::Fixer::ReorderResult&) {}
    virtual void post_remove_data(std::shared_ptr<segment::Fixer>, segment::Fixer::MarkRemovedResult&) {}
    virtual void pre_remove(std::shared_ptr<segment::Fixer>) {}
    virtual void post_convert(std::shared_ptr<segment::Fixer>, segment::Fixer::ConvertResult&) {}

public:
    std::shared_ptr<core::CheckLock> lock;
    std::shared_ptr<const Segment> segment;
    std::shared_ptr<segment::Checker> segment_checker;
    std::shared_ptr<segment::Data> segment_data;
    std::shared_ptr<segment::data::Checker> segment_data_checker;

    CheckerSegment(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock);
    virtual ~CheckerSegment();

    /// Convert the segment into a tar segment
    virtual segment::Fixer::ConvertResult tar();

    /// Convert the segment into a zip segment
    virtual segment::Fixer::ConvertResult zip();

    /**
     * Compress the segment
     *
     * Returns the size difference between the original size and the compressed
     * size
     */
    virtual segment::Fixer::ConvertResult compress(unsigned groupsize);

    virtual std::filesystem::path path_relative() const = 0;
    virtual const segmented::Dataset& dataset() const = 0;
    virtual segmented::Dataset& dataset() = 0;
    virtual std::shared_ptr<dataset::archive::Checker> archives() = 0;

    virtual SegmentState fsck(dataset::Reporter& reporter, bool quick=true) = 0;

    /**
     * Remove entries from this segment, indicated by their stating offsets.
     *
     * Return the total size of data deleted. The space may not be freed right
     * away, and may need to be reclaimed by a repack operation
     */
    virtual segment::Fixer::MarkRemovedResult remove_data(const std::set<uint64_t>& offsets);

    /**
     * Optimise the contents of a data file
     *
     * In the resulting file, there are no holes for deleted data and all
     * the data is sorted by reference time
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual segment::Fixer::ReorderResult repack(unsigned test_flags=0);

    /**
     * Remove the segment
     */
    virtual size_t remove(bool with_data=false);

    /**
     * Add information about a file to the index
     */
    virtual void index(metadata::Collection&& contents) = 0;

    /**
     * Consider all existing metadata about a file as invalid and rebuild
     * them by rescanning the file
     */
    virtual void rescan(dataset::Reporter& reporter) = 0;

    /**
     * Release the segment from the dataset and move it to destpath.
     *
     * Destpath must be on the same filesystem as the segment.
     */
    virtual arki::metadata::Collection release(std::shared_ptr<const segment::Session> new_segment_session, const std::filesystem::path& new_relpath) = 0;

    /**
     * Move the file to archive
     *
     * The default implementation moves the file and its associated
     * metadata and summaries (if found) to the "last" archive, and adds it
     * to its manifest
     */
    virtual void archive();

    /**
     * Move a segment from the last/ archive back to the main dataset
     */
    virtual void unarchive();
};


/**
 * LocalChecker with data stored in segment files
 */
class Checker : public local::Checker
{
protected:
    /**
     * Instantiate a CheckerSegment from a relative segment path
     *
     * This is only intended for use in tests.
     */
    std::unique_ptr<CheckerSegment> segment_from_relpath(const std::filesystem::path& relpath);

public:
    using local::Checker::Checker;
    ~Checker();

    const Dataset& dataset() const override = 0;
    Dataset& dataset() override = 0;

    void check(CheckerConfig& opts) override;
    void repack(CheckerConfig& opts, unsigned test_flags=0) override;
    void remove(const metadata::Collection& mds) override;


    /// Instantiate a CheckerSegment
    virtual std::unique_ptr<CheckerSegment> segment(std::shared_ptr<const Segment> segment) = 0;

    /// Instantiate a CheckerSegment using an existing lock
    virtual std::unique_ptr<CheckerSegment> segment_prelocked(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock) = 0;

    /**
     * List all segments known to this dataset
     */
    virtual void segments_tracked(std::function<void(CheckerSegment& segment)>) = 0;

    /**
     * List all segments known to this dataset
     */
    virtual void segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) = 0;

    /**
     * List all segments present on disk but not known to this dataset
     */
    virtual void segments_untracked(std::function<void(segmented::CheckerSegment& segment)>) = 0;

    /**
     * List all segments known to this dataset
     */
    virtual void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) = 0;

    /**
     * List all segments, both known to this dataset or unknown but found on disk
     */
    void segments(CheckerConfig& config, std::function<void(segmented::CheckerSegment& segment)>);

    /**
     * List all segments, both known to this dataset or unknown but found on disk
     */
    void segments_all(std::function<void(segmented::CheckerSegment& segment)>);

    /**
     * List all segments, both known to the dataset and unknown but found on
     * disk, for this dataset and all its archives
     */
    void segments_recursive(CheckerConfig& opts, std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest);

    void remove_old(CheckerConfig& opts) override;
    void remove_all(CheckerConfig& opts) override;
    void tar(CheckerConfig& config) override;
    void zip(CheckerConfig& config) override;
    void compress(CheckerConfig& config, unsigned groupsize) override;
    void state(CheckerConfig& config) override;

    /**
     * Perform generic packing and optimisations
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t vacuum(dataset::Reporter& reporter) = 0;

    /**
     * Set the modification time for all contents of the dataset
     */
    virtual void test_touch_contents(time_t timestamp);

    /**
     * All data in the segment except the `data_idx`-one are shifted backwards
     * by `overlap_size`, so that one in position `data_idx-1` overlaps with
     * the one in position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx=1);

    /**
     * All data in the segment starting from the one at position `data_idx` are
     * shifted forwards by `hole_size` offset positions, so that a gap is
     * formed before the element at position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx=0);

    /**
     * Corrupt the data in the given segment at position `data_idx`, by
     * replacing its first byte with the value 0.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_corrupt_data(const std::filesystem::path& relpath, unsigned data_idx=0);

    /**
     * Truncate the segment at position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_truncate_data(const std::filesystem::path& relpath, unsigned data_idx=0);

    /**
     * Swap the data in the segment at position `d1_idx` with the one at
     * position `d2_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_swap_data(const std::filesystem::path& relpath, unsigned d1_idx, unsigned d2_idx);

    /**
     * Rename the segment, leaving its contents unchanged.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath);

    /**
     * Replace the metadata for the data in the segment at position `data_idx`.
     *
     * The source of the metadata will be preserved. md will be updated to
     * point to the final metadata.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual metadata::Collection test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx=0);

    /**
     * Remove all index data for the given segment, leaving the index valid. It
     * is the equivalent of deleting all data from this segment from the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_delete_from_index(const std::filesystem::path& relpath);

    /**
     * Remove all index data for the given segment, but make it look as if the
     * index had accidentally been removed and regenerated, and should not be
     * trusted as a source to mark the segment as deleted.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_invalidate_in_index(const std::filesystem::path& relpath) = 0;

    /**
     * Scan a dataset for data files, returning a set of pathnames relative to
     * root.
     */
    void scan_dir(std::function<void(std::shared_ptr<const Segment> segment)> dest);
};


struct TestHooks
{
    std::function<void(const Segment&)> on_check_lock;
    std::function<void(const Segment&)> on_repack_write_lock;

    TestHooks();
};

}
#endif
