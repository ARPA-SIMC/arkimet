#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/dataset/segment.h>

namespace arki {
namespace dataset {
class Step;

struct SegmentedBase
{
protected:
    segment::SegmentManager* m_segment_manager;

public:
    SegmentedBase(const ConfigFile& cfg);
    ~SegmentedBase();
};

/**
 * LocalReader dataset with data stored in segment files
 */
class SegmentedReader : public LocalReader, public SegmentedBase
{
public:
    SegmentedReader(const ConfigFile& cfg);
    ~SegmentedReader();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class SegmentedWriter : public LocalWriter, public SegmentedBase
{
protected:
    ReplaceStrategy m_default_replace_strategy;
    Step* m_step;

    /**
     * Return an instance of the Segment for the file where the given metadata
     * should be written
     */
    segment::Segment* file(const Metadata& md, const std::string& format);

public:
    SegmentedWriter(const ConfigFile& cfg);
    ~SegmentedWriter();

    virtual void flush();

    /**
     * Instantiate an appropriate Writer for the given configuration
     */
    static SegmentedWriter* create(const ConfigFile& cfg);

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

/**
 * LocalChecker with data stored in segment files
 */
class SegmentedChecker : public LocalChecker, public SegmentedBase
{
protected:
    Step* m_step;

public:
    SegmentedChecker(const ConfigFile& cfg);
    ~SegmentedChecker();

    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    /**
     * Instantiate an appropriate SegmentedChecker for the given configuration
     */
    static SegmentedChecker* create(const ConfigFile& cfg);

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
    virtual void maintenance(segment::state_func dest, bool quick=true);

    /// Remove all data from the dataset
    void removeAll(dataset::Reporter& reporter, bool writable) override;

    /**
     * Add information about a file to the index
     */
    virtual void indexFile(const std::string& relpath, metadata::Collection&& contents) = 0;

    /**
     * Consider all existing metadata about a file as invalid and rebuild
     * them by rescanning the file
     */
    virtual void rescanFile(const std::string& relpath) = 0;

    /**
     * Optimise the contents of a data file
     *
     * In the resulting file, there are no holes for deleted data and all
     * the data is sorted by reference time
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t repackFile(const std::string& relpath) = 0;

    /**
     * Remove the file from the dataset
     *
     * @returns The number of bytes freed on disk with this operation
     */
    virtual size_t removeFile(const std::string& relpath, bool withData=false) = 0;

    /**
     * Move the file to archive
     *
     * The default implementation moves the file and its associated
     * metadata and summaries (if found) to the "last" archive, and adds it
     * to its manifest
     */
    virtual void archiveFile(const std::string& relpath);

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
