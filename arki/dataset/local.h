#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/// dataset/local - Base class for local datasets

#include <arki/dataset.h>
#include <arki/core/fwd.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {
class Lock;
class ReadLock;
class AppendLock;
class CheckLock;
class LocalConfig;
class ArchivesConfig;
class ArchivesReader;
class ArchivesChecker;

class LocalConfig : public Config
{
protected:
    mutable std::shared_ptr<ArchivesConfig> m_archives_config;

public:
    /// Root path of the dataset
    std::string path;

    int archive_age = -1;
    int delete_age = -1;

    const core::lock::Policy* lock_policy;

    LocalConfig(const ConfigFile& cfg);

    /**
     * Check if the data to be acquired is older than acquire or delete age.
     *
     * If it is, return true and the import result value.
     *
     * If it is not, returns false and WriterAcquireResult should be ignored.
     */
    std::pair<bool, WriterAcquireResult> check_acquire_age(Metadata& md) const;

    std::shared_ptr<ArchivesConfig> archives_config() const;

    /**
     * Create/open a dataset-wide lockfile, returning the Lock instance
     */
    std::shared_ptr<dataset::ReadLock> read_lock_dataset() const;
    std::shared_ptr<dataset::AppendLock> append_lock_dataset() const;
    std::shared_ptr<dataset::CheckLock> check_lock_dataset() const;
    std::shared_ptr<dataset::ReadLock> read_lock_segment(const std::string& relpath) const;
    std::shared_ptr<dataset::AppendLock> append_lock_segment(const std::string& relpath) const;
    std::shared_ptr<dataset::CheckLock> check_lock_segment(const std::string& relpath) const;
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
    bool query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;

    // Base implementations that queries the archives if they exist
    void query_summary(const Matcher& matcher, Summary& summary) override;

    static void readConfig(const std::string& path, ConfigFile& cfg);
};

class LocalWriter : public Writer
{
public:
    using Writer::Writer;
    ~LocalWriter();

    const LocalConfig& config() const override = 0;

    /// Return the dataset path
    const std::string& path() const { return config().path; }

    static void test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out);
};

struct LocalChecker : public LocalBase<Checker, ArchivesChecker>
{
public:
    using LocalBase::LocalBase;
    ~LocalChecker();

    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
    void check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick) override;
    void remove_all(CheckerConfig& opts) override;
    void check_issue51(CheckerConfig& opts) override;
    void tar(CheckerConfig& opts) override;
};

}
}
#endif
