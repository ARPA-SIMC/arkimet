#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/segment.h>
#include <arki/core/time.h>

namespace arki {
namespace dataset {
class Step;

namespace segmented {

struct Config : public LocalConfig
{
protected:
    /// dataset::Step for this configuration
    std::shared_ptr<Step> m_step;

    Config(const Config& cfg) = default;

public:
    /// Name of the dataset::Step used to dispatch data into segments
    std::string step_name;

    /// What replace strategy to use when acquire() is called with REPLACE_DEFAULT
    dataset::Writer::ReplaceStrategy default_replace_strategy;

    /**
     * If false, autodetect segment types base on data types.
     *
     * If true, directory segments are always used regardless of data type.
     */
    bool force_dir_segments = false;

    /**
     * If true, segments on disk will contain holes and allocate no disk space.
     *
     * This is only used for testing.
     */
    bool mock_data = false;

    /**
     * If true, this dataset is used as an archive for offline data
     */
    bool offline = false;

    Config(const ConfigFile& cfg);
    ~Config();

    virtual bool relpath_timespan(const std::string& path, core::Time& start_time, core::Time& end_time) const;

    const Step& step() const { return *m_step; }

    std::unique_ptr<SegmentManager> create_segment_manager() const;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

/**
 * LocalReader dataset with data stored in segment files
 */
class Reader : public LocalReader
{
private:
    SegmentManager* m_segment_manager = nullptr;

public:
    using LocalReader::LocalReader;
    ~Reader();

    const Config& config() const override = 0;
    SegmentManager& segment_manager();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class Writer : public LocalWriter
{
private:
    SegmentManager* m_segment_manager = nullptr;

protected:
    /**
     * Return an instance of the Segment for the file where the given metadata
     * should be written
     */
    std::shared_ptr<segment::Writer> file(const Metadata& md, const std::string& format);

    std::map<std::string, WriterBatch> batch_by_segment(WriterBatch& batch);

public:
    using LocalWriter::LocalWriter;
    ~Writer();

    const Config& config() const override = 0;
    SegmentManager& segment_manager();

    static void test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out);
};


/**
 * State of a segment in the dataset
 */
struct SegmentState
{
    // Segment state
    arki::segment::State state;
    // Minimum reference time of data that can fit in the segment
    core::Time begin;
    // Maximum reference time of data that can fit in the segment
    core::Time until;

    SegmentState(arki::segment::State state)
        : state(state), begin(0, 0, 0), until(0, 0, 0) {}
    SegmentState(arki::segment::State state, const core::Time& begin, const core::Time& until)
        : state(state), begin(begin), until(until) {}
    SegmentState(const SegmentState&) = default;
    SegmentState(SegmentState&&) = default;

    /// Check if this segment is old enough to be deleted or archived
    void check_age(const std::string& relpath, const Config& cfg, dataset::Reporter& reporter);
};


class CheckerSegment
{
public:
    std::shared_ptr<dataset::CheckLock> lock;
    std::shared_ptr<segment::Checker> segment;

    CheckerSegment(std::shared_ptr<dataset::CheckLock> lock);
    virtual ~CheckerSegment();

    /**
     * Get the metadata for the contents of this segment known to the dataset
     */
    virtual void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) = 0;

    /// Convert the segment into a tar segment
    virtual void tar() = 0;

    /// Convert the segment into a zip segment
    virtual void zip() = 0;

    /**
     * Compress the segment
     *
     * Returns the size difference between the original size and the compressed
     * size
     */
    virtual size_t compress() = 0;

    virtual std::string path_relative() const = 0;
    virtual const segmented::Config& config() const = 0;
    virtual dataset::ArchivesChecker& archives() const = 0;

    virtual SegmentState scan(dataset::Reporter& reporter, bool quick=true) = 0;

    /**
     * Optimise the contents of a data file
     *
     * In the resulting file, there are no holes for deleted data and all
     * the data is sorted by reference time
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t repack(unsigned test_flags=0) = 0;

    /**
     * Rewrite the segment so that the data has the same order as `mds`.
     *
     * In the resulting file, there are no holes between data.
     *
     * @returns The size difference between the initial segment size and the
     * final segment size.
     */
    virtual size_t reorder(metadata::Collection& mds, unsigned test_flags=0) = 0;

    /**
     * Remove the segment
     */
    virtual size_t remove(bool with_data=false) = 0;

    /**
     * Add information about a file to the index
     */
    virtual void index(metadata::Collection&& contents) = 0;

    /**
     * Consider all existing metadata about a file as invalid and rebuild
     * them by rescanning the file
     */
    virtual void rescan() = 0;

    /**
     * Release the segment from the dataset and move it to destpath.
     *
     * Destpath must be on the same filesystem as the segment.
     */
    virtual void release(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) = 0;

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
class Checker : public LocalChecker
{
private:
    SegmentManager* m_segment_manager = nullptr;

public:
    using LocalChecker::LocalChecker;
    ~Checker();

    const Config& config() const override = 0;
    SegmentManager& segment_manager();

    void check(CheckerConfig& opts) override;
    void repack(CheckerConfig& opts, unsigned test_flags=0) override;

    /// Instantiate a CheckerSegment
    virtual std::unique_ptr<CheckerSegment> segment(const std::string& relpath) = 0;

    /// Instantiate a CheckerSegment using an existing lock
    virtual std::unique_ptr<CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) = 0;

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
     * List all segments, both known to this dataset or unknown but found on disk
     */
    void segments_all_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>);

    /**
     * List all segments, both known to the dataset and unknown but found on
     * disk, for this dataset and all its archives
     */
    void segments_recursive(CheckerConfig& opts, std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest);

    void remove_old(CheckerConfig& opts) override;
    void remove_all(CheckerConfig& opts) override;
    void tar(CheckerConfig& config) override;
    void zip(CheckerConfig& config) override;
    void compress(CheckerConfig& config) override;
    void state(CheckerConfig& config) override;

    /**
     * Perform generic packing and optimisations
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t vacuum(dataset::Reporter& reporter) = 0;

    /**
     * All data in the segment except the `data_idx`-one are shifted backwards
     * by `overlap_size`, so that one in position `data_idx-1` overlaps with
     * the one in position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx=1) = 0;

    /**
     * All data in the segment starting from the one at position `data_idx` are
     * shifted forwards by `hole_size` offset positions, so that a gap is
     * formed before the element at position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx=0) = 0;

    /**
     * Corrupt the data in the given segment at position `data_idx`, by
     * replacing its first byte with the value 0.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_corrupt_data(const std::string& relpath, unsigned data_idx=0) = 0;

    /**
     * Truncate the segment at position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_truncate_data(const std::string& relpath, unsigned data_idx=0) = 0;

    /**
     * Swap the data in the segment at position `d1_idx` with the one at
     * position `d2_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx) = 0;

    /**
     * Rename the segment, leaving its contents unchanged.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_rename(const std::string& relpath, const std::string& new_relpath) = 0;

    /**
     * Replace the metadata for the data in the segment at position `data_idx`.
     *
     * The source of the metadata will be preserved. md will be updated to
     * point to the final metadata.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx=0) = 0;

    /**
     * Remove index data for the given segment making it as if it was never
     * imported.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_remove_index(const std::string& relpath) = 0;
};

}
}
}
#endif
