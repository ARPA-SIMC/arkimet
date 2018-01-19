#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/dataset/segment.h>
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

    Config(const ConfigFile& cfg);
    ~Config();

    virtual bool relpath_timespan(const std::string& path, core::Time& start_time, core::Time& end_time) const;

    const Step& step() const { return *m_step; }

    std::unique_ptr<segment::Manager> create_segment_manager() const;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

/**
 * LocalReader dataset with data stored in segment files
 */
class Reader : public LocalReader
{
private:
    segment::Manager* m_segment_manager = nullptr;

public:
    using LocalReader::LocalReader;
    ~Reader();

    const Config& config() const override = 0;
    segment::Manager& segment_manager();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class Writer : public LocalWriter
{
private:
    segment::Manager* m_segment_manager = nullptr;

protected:
    /**
     * Return an instance of the Segment for the file where the given metadata
     * should be written
     */
    std::shared_ptr<segment::Writer> file(const Metadata& md, const std::string& format);

public:
    using LocalWriter::LocalWriter;
    ~Writer();

    const Config& config() const override = 0;
    segment::Manager& segment_manager();

    virtual void flush();

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};


/**
 * State of a segment in the dataset
 */
struct SegmentState
{
    // Segment state
    segment::State state;
    // Minimum reference time of data that can fit in the segment
    core::Time begin;
    // Maximum reference time of data that can fit in the segment
    core::Time until;

    SegmentState(segment::State state)
        : state(state), begin(0, 0, 0), until(0, 0, 0) {}
    SegmentState(segment::State state, const core::Time& begin, const core::Time& until)
        : state(state), begin(begin), until(until) {}
    SegmentState(const SegmentState&) = default;
    SegmentState(SegmentState&&) = default;

    /// Check if this segment is old enough to be deleted or archived
    void check_age(const std::string& relpath, const Config& cfg, dataset::Reporter& reporter);
};


/**
 * State of all segments in the dataset
 */
struct State : public std::map<std::string, SegmentState>
{
    using std::map<std::string, SegmentState>::map;

    bool has(const std::string& relpath) const;

    const SegmentState& get(const std::string& seg) const;

    /// Count how many segments have this state
    unsigned count(segment::State state) const;

    void dump(FILE* out) const;
};


class CheckerSegment
{
public:
    std::shared_ptr<dataset::CheckLock> lock;
    std::shared_ptr<segment::Checker> segment;

    CheckerSegment(std::shared_ptr<dataset::CheckLock> lock);
    virtual ~CheckerSegment();

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
    virtual void release(const std::string& destpath) = 0;

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
    segment::Manager* m_segment_manager = nullptr;

public:
    using LocalChecker::LocalChecker;
    ~Checker();

    const Config& config() const override = 0;
    segment::Manager& segment_manager();

    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick) override;

    /**
     * Scan the dataset, computing the state of each unarchived segment that is
     * either on disk or known by the index.
     */
    State scan(dataset::Reporter& reporter, bool quick=true);

    /// Same as scan, but limited to segments matching the given matcher
    State scan_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool quick=true);

    /// Instantiate a CheckerSegment
    virtual std::unique_ptr<CheckerSegment> segment(const std::string& relpath) = 0;

    /// Instantiate a CheckerSegment using an existing lock
    virtual std::unique_ptr<CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) = 0;

    /**
     * List all segments known to this dataset
     */
    virtual void segments(std::function<void(CheckerSegment& segment)>) = 0;

    /**
     * List all segments known to this dataset
     */
    virtual void segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) = 0;

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
    void segments_all(std::function<void(segmented::CheckerSegment& segment)>);

    /**
     * List all segments, both known to this dataset or unknown but found on disk
     */
    void segments_all_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>);

    /// Remove all data from the dataset
    void remove_all(dataset::Reporter& reporter, bool writable=false) override;

    /// Remove all data from the dataset
    void remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false) override;

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
