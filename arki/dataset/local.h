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

template<typename Parent, typename Archives>
class LocalBase : public Parent
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
class LocalReader : public LocalBase<Reader, ArchivesReader>
{
public:
    LocalReader(const ConfigFile& cfg);
    ~LocalReader();

    // Base implementations that queries the archives if they exist
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;

    // Base implementations that queries the archives if they exist
    void query_summary(const Matcher& matcher, Summary& summary) override;

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

struct LocalChecker : public LocalBase<Checker, ArchivesChecker>
{
    LocalChecker(const ConfigFile& cfg);
    ~LocalChecker();

    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    /**
     * Instantiate an appropriate Dataset for the given configuration
     */
    static LocalChecker* create(const ConfigFile& cfg);
};

}
}
#endif
