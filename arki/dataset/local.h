#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/// dataset/local - Base class for local datasets

#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {
class TargetFile;
class Archives;
class Index;

namespace data {
class SegmentManager;
class Segment;
}

namespace maintenance {
class MaintFileVisitor;
}

/**
 * Base class for local datasets
 */
class LocalReader : public ReadonlyDataset
{
protected:
    std::string m_name;
    std::string m_path;
    mutable Archives* m_archive;

public:
    LocalReader(const ConfigFile& cfg);
    ~LocalReader();

    // Return the dataset name
    const std::string& name() const { return m_name; }

    // Return the dataset path
    const std::string& path() const { return m_path; }

    // Base implementations that queries the archives if they exist
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;

    // Base implementations that queries the archives if they exist
    void querySummary(const Matcher& matcher, Summary& summary) override;

    /**
     * For each file in the archive, output to \a cons the data at position
     * \a * idx
     *
     * @return the number of data produced. If 0, then all files in the archive
     * have less than \a idx data inside.
     */
    virtual size_t produce_nth(metadata_dest_func cons, size_t idx=0);

    /**
     * For each file in the archive, rescan the \a idx data in it and and check
     * if the result still fits with the dataset matcher.
     *
     * Send all the mismatching metadata to \a cons
     *
     * The base implementation only runs the scan_test in the archives if they
     * exist
     *
     * @return the number of data scanned at this idx, or 0 if no files in the
     * dataset have at least \a idx elements inside
     */
    size_t scan_test(metadata_dest_func cons, size_t idx=0);

    bool hasArchive() const;
    Archives& archive();
    const Archives& archive() const;

    static void readConfig(const std::string& path, ConfigFile& cfg);
};


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


/// SegmentedReader that can make use of an index
class IndexedReader : public SegmentedReader
{
protected:
    Index* m_idx = nullptr;

public:
    IndexedReader(const ConfigFile& cfg);
    ~IndexedReader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    size_t produce_nth(metadata_dest_func cons, size_t idx=0) override;

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const { return m_idx != 0; }
};


class LocalWriter : public WritableDataset
{
protected:
    std::string m_path;
    mutable Archives* m_archive;
    int m_archive_age;
    int m_delete_age;

public:
    LocalWriter(const ConfigFile& cfg);
    ~LocalWriter();

    // Return the dataset path
    const std::string& path() const { return m_path; }

    bool hasArchive() const;
    Archives& archive();
    const Archives& archive() const;

    /**
     * Repack the dataset, logging status to the given file.
     *
     * If writable is false, the process is simulated but no changes are
     * saved.
     */
    virtual void repack(std::ostream& log, bool writable=false) = 0;

    /**
     * Check the dataset for errors, logging status to the given file.
     *
     * If \a fix is false, the process is simulated but no changes are saved.
     * If \a fix is true, errors are fixed.
     */
    virtual void check(std::ostream& log, bool fix, bool quick) = 0;

    /**
     * Instantiate an appropriate Dataset for the given configuration
     */
    static LocalWriter* create(const ConfigFile& cfg);

    /**
     * Simulate acquiring the given metadata item (and related data) in this
     * dataset.
     *
     * No change of any kind happens to the dataset.  Information such as the
     * dataset name and the id of the data in the dataset are added to the
     * Metadata object.
     *
     * @return The outcome of the operation.
     */
    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

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
    virtual void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);

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
};

}
}
#endif
