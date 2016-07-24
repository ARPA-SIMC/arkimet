#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/dataset/segment.h>

namespace arki {
namespace dataset {
class Step;

struct SegmentedConfig : public LocalConfig
{
protected:
    /// dataset::Step for this configuration
    std::shared_ptr<Step> m_step;

    SegmentedConfig(const SegmentedConfig& cfg) = default;

    void to_shard(const std::string& shard_path, std::shared_ptr<Step> step);

public:
    /// Name of the dataset::Step used to dispatch data into segments
    std::string step_name;

    /// What replace strategy to use when acquire() is called with REPLACE_DEFAULT
    Writer::ReplaceStrategy default_replace_strategy;

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

    SegmentedConfig(const ConfigFile& cfg);
    ~SegmentedConfig();

    const Step& step() const { return *m_step; }

    std::unique_ptr<segment::SegmentManager> create_segment_manager() const;

    static std::shared_ptr<const SegmentedConfig> create(const ConfigFile& cfg);
};

/**
 * LocalReader dataset with data stored in segment files
 */
class SegmentedReader : public LocalReader
{
private:
    segment::SegmentManager* m_segment_manager = nullptr;

public:
    using LocalReader::LocalReader;
    ~SegmentedReader();

    const SegmentedConfig& config() const override = 0;
    segment::SegmentManager& segment_manager();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class SegmentedWriter : public LocalWriter
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
    ~SegmentedWriter();

    const SegmentedConfig& config() const override = 0;
    segment::SegmentManager& segment_manager();

    virtual void flush();

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

/**
 * LocalChecker with data stored in segment files
 */
class SegmentedChecker : public LocalChecker
{
private:
    segment::SegmentManager* m_segment_manager = nullptr;

public:
    using LocalChecker::LocalChecker;
    ~SegmentedChecker();

    const SegmentedConfig& config() const override = 0;
    segment::SegmentManager& segment_manager();

    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    /**
     * Perform dataset maintenance, sending information to \a v
     *
     * Subclassers should call LocalWriter's maintenance method at the
     * end of their own maintenance, as it takes care of performing
     * maintainance of archives, if present.
     *
     * @params v
     *   The visitor-style class that gets notified of the state of the
     *   various files in the dataset
     * @params quick
     *   If false, contents of the data files will also be checked for
     *   consistency
     */
    virtual void maintenance(dataset::Reporter& reporter, segment::state_func dest, bool quick=true);

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
    virtual size_t repackSegment(const std::string& relpath) = 0;

    /**
     * Remove the file from the dataset
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t removeSegment(const std::string& relpath, bool withData=false) = 0;

    /**
     * Move the file to archive
     *
     * The default implementation moves the file and its associated
     * metadata and summaries (if found) to the "last" archive, and adds it
     * to its manifest
     */
    virtual void archiveSegment(const std::string& relpath);

    /**
     * Perform generic packing and optimisations
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t vacuum() = 0;
};

}
}
#endif
