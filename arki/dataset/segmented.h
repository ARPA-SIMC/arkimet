#ifndef ARKI_DATASET_SEGMENTED_H
#define ARKI_DATASET_SEGMENTED_H

#include <arki/dataset/local.h>
#include <arki/dataset/data.h>

namespace arki {
namespace dataset {
class TargetFile;

namespace data {
class SegmentManager;
class Segment;
}


/**
 * LocalReader dataset with data stored in segment files
 */
class SegmentedReader : public LocalReader
{
protected:
    data::SegmentManager* m_segment_manager;

public:
    SegmentedReader(const ConfigFile& cfg);
    ~SegmentedReader();
};

/**
 * LocalWriter dataset with data stored in segment files
 */
class SegmentedWriter : public LocalWriter
{
protected:
    ReplaceStrategy m_default_replace_strategy;
    TargetFile* m_tf;
    data::SegmentManager* m_segment_manager;

    /**
     * Return an instance of the Segment for the file where the given metadata
     * should be written
     */
    data::Segment* file(const Metadata& md, const std::string& format);

public:
    SegmentedWriter(const ConfigFile& cfg);
    ~SegmentedWriter();

    void repack(std::ostream& log, bool writable=false) override;
    void check(std::ostream& log, bool fix, bool quick) override;

    virtual void flush();

    // Maintenance functions

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
    virtual void maintenance(data::state_func dest, bool quick=true);

    /**
     * Perform general sanity checks on the dataset, reporting to \a log.
     *
     * If \a writable is true, try to fix issues.
     */
    virtual void sanityChecks(std::ostream& log, bool writable=false);

    /// Remove all data from the dataset
    void removeAll(std::ostream& log, bool writable);

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

    /**
     * Instantiate an appropriate Dataset for the given configuration
     */
    static SegmentedWriter* create(const ConfigFile& cfg);

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};


}
}

#endif
