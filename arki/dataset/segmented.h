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

    void to_shard(const std::string& shard_path, std::shared_ptr<Step> step);

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

    std::unique_ptr<segment::SegmentManager> create_segment_manager() const;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

/**
 * LocalReader dataset with data stored in segment files
 */
class Reader : public LocalReader
{
private:
    segment::SegmentManager* m_segment_manager = nullptr;

public:
    using LocalReader::LocalReader;
    ~Reader();

    const Config& config() const override = 0;
    segment::SegmentManager& segment_manager();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class Writer : public LocalWriter
{
private:
    segment::SegmentManager* m_segment_manager = nullptr;

protected:
    /**
     * Return an instance of the Segment for the file where the given metadata
     * should be written
     */
    Segment* file(const Metadata& md, const std::string& format);

public:
    using LocalWriter::LocalWriter;
    ~Writer();

    const Config& config() const override = 0;
    segment::SegmentManager& segment_manager();

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
};


/**
 * State of all segments in the dataset
 */
struct State : public std::map<std::string, SegmentState>
{
    using std::map<std::string, SegmentState>::map;

    const SegmentState& get(const std::string& seg) const;

    void dump(FILE* out) const;
};


/**
 * LocalChecker with data stored in segment files
 */
class Checker : public LocalChecker
{
private:
    segment::SegmentManager* m_segment_manager = nullptr;

public:
    using LocalChecker::LocalChecker;
    ~Checker();

    const Config& config() const override = 0;
    segment::SegmentManager& segment_manager();

    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    /**
     * Scan the dataset, computing the state of each unarchived segment that is
     * either on disk or known by the index.
     */
    virtual State scan(dataset::Reporter& reporter, bool quick=true) = 0;

    /// Remove all data from the dataset
    void removeAll(dataset::Reporter& reporter, bool writable) override;

    /**
     * Add information about a file to the index
     */
    virtual void indexSegment(const std::string& relpath, metadata::Collection&& contents) = 0;

    /**
     * Consider all existing metadata about a file as invalid and rebuild
     * them by rescanning the file
     */
    virtual void rescanSegment(const std::string& relpath) = 0;

    /**
     * Optimise the contents of a data file
     *
     * In the resulting file, there are no holes for deleted data and all
     * the data is sorted by reference time
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t repackSegment(const std::string& relpath, unsigned test_flags=0) = 0;

    /**
     * Remove the file from the dataset
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t removeSegment(const std::string& relpath, bool withData=false) = 0;

    /**
     * Release the segment from the dataset and move it to destpath.
     *
     * Destpath must be on the same filesystem as the segment.
     */
    virtual void releaseSegment(const std::string& relpath, const std::string& destpath) = 0;

    /**
     * Move the file to archive
     *
     * The default implementation moves the file and its associated
     * metadata and summaries (if found) to the "last" archive, and adds it
     * to its manifest
     */
    virtual void archiveSegment(const std::string& relpath);

    /**
     * Move a segment from the last/ archive back to the main dataset
     */
    virtual void unarchive_segment(const std::string& relpath);

    /**
     * Perform generic packing and optimisations
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t vacuum() = 0;

    /**
     * All data in the segment except the `data_idx`-one are shifted backwards
     * by one, so that one in position `data_idx-1` overlaps with the one in
     * position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_overlap(const std::string& relpath, unsigned data_idx=1) = 0;

    /**
     * All data in the segment starting from the one at position `data_idx` are
     * shifted forwards by one offset position, so that a gap is formed before
     * the element at position `data_idx`.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_make_hole(const std::string& relpath, unsigned data_idx=0) = 0;

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
     * Remove the segment from the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     */
    virtual void test_deindex(const std::string& relpath) = 0;
};

}
}
}
#endif
