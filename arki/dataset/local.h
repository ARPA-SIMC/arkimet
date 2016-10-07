#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/// dataset/local - Base class for local datasets

#include <arki/dataset.h>
#include <string>
#include <arki/file.h>
#include <fcntl.h>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {
class LocalConfig;
class ArchivesConfig;
class ArchivesReader;
class ArchivesChecker;

class LocalConfig : public Config
{
protected:
    mutable std::shared_ptr<ArchivesConfig> m_archives_config;

    void to_shard(const std::string& shard_path);

public:
    /// Root path of the dataset
    std::string path;

    /// Pathname of the dataset's lock file
    std::string lockfile_pathname;

    int archive_age = -1;
    int delete_age = -1;

    LocalConfig(const ConfigFile& cfg);

    std::shared_ptr<ArchivesConfig> archives_config() const;
};

template<typename Parent, typename Archives>
class LocalBase : public Parent
{
protected:
    Archives* m_archive = nullptr;

public:
    using Parent::Parent;
    ~LocalBase();

    const LocalConfig& config() const override = 0;

    /// Return the dataset path
    const std::string& path() const { return config().path; }

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
    using LocalBase::LocalBase;
    ~LocalReader();

    // Base implementations that queries the archives if they exist
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;

    // Base implementations that queries the archives if they exist
    void query_summary(const Matcher& matcher, Summary& summary) override;

    static void readConfig(const std::string& path, ConfigFile& cfg);
};

struct LocalLock
{
    struct flock ds_lock;
    bool locked = false;
    arki::File lockfile;

    LocalLock(const std::string& pathname);
    ~LocalLock();

    void acquire();
    void release();
};

class LocalWriter : public Writer
{
protected:
    LocalLock* lock = nullptr;
    void acquire_lock();
    void release_lock();

    /**
     * Check if the data to be acquired is older than acquire or delete age.
     *
     * If it is, return true and the import result value.
     *
     * If it is not, returns false and AcquireResult should be ignored.
     */
    std::pair<bool, AcquireResult> check_acquire_age(Metadata& md) const;

public:
    using Writer::Writer;
    ~LocalWriter();

    const LocalConfig& config() const override = 0;

    /// Return the dataset path
    const std::string& path() const { return config().path; }

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
protected:
    LocalLock* lock = nullptr;
    void acquire_lock();
    void release_lock();

public:
    using LocalBase::LocalBase;
    ~LocalChecker();

    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
    void check_issue51(dataset::Reporter& reporter, bool fix=false) override;
};

}
}
#endif
