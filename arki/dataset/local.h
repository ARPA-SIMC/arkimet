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
class ArchivesReader;
class ArchivesChecker;

template<typename Archives>
class LocalBase
{
protected:
    std::string m_path;
    Archives* m_archive = nullptr;
    int m_archive_age = -1;
    int m_delete_age = -1;

public:
    LocalBase(const ConfigFile& cfg);
    ~LocalBase();

    /// Return the dataset path
    const std::string& path() const { return m_path; }

    /// Check if the dataset has archived data
    bool hasArchive() const;

    /// Return the Archives for this dataset
    Archives& archive();
};

/**
 * Base class for local datasets
 */
class LocalReader : public Reader, public LocalBase<ArchivesReader>
{
public:
    LocalReader(const ConfigFile& cfg);
    ~LocalReader();

    // Base implementations that queries the archives if they exist
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;

    // Base implementations that queries the archives if they exist
    void query_summary(const Matcher& matcher, Summary& summary) override;

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

    static void readConfig(const std::string& path, ConfigFile& cfg);
};

class LocalWriter : public Writer
{
protected:
    std::string m_path;

public:
    LocalWriter(const ConfigFile& cfg);
    ~LocalWriter();

    /// Return the dataset path
    const std::string& path() const { return m_path; }

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

struct LocalChecker : public Checker, public LocalBase<ArchivesChecker>
{
    LocalChecker(const ConfigFile& cfg);
    ~LocalChecker();

    /**
     * Instantiate an appropriate Dataset for the given configuration
     */
    static LocalChecker* create(const ConfigFile& cfg);
};

}
}
#endif
