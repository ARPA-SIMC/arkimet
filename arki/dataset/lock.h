#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki {
namespace dataset {
struct LocalConfig;

struct Lock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;

    Lock(const std::string& pathname, const core::lock::Policy* lock_policy);

    virtual std::shared_ptr<core::Lock> write_lock() = 0;
};


struct ReadLock : public Lock
{
    std::weak_ptr<core::Lock> current_write_lock;

    ReadLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~ReadLock();

    /**
     * Escalate a read lock to a write lock as long as the resulting lock is in
     * use
     */
    std::shared_ptr<core::Lock> write_lock() override;
};


struct WriteLock : public Lock
{
    WriteLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~WriteLock();

    /**
     * Return a reference to self
     */
    std::shared_ptr<core::Lock> write_lock() override;
};



struct DatasetReadLock : public ReadLock
{
    DatasetReadLock(const LocalConfig& config);
};

struct DatasetWriteLock : public WriteLock
{
    DatasetWriteLock(const LocalConfig& config);
};

struct SegmentReadLock : public ReadLock
{
    SegmentReadLock(const LocalConfig& config, const std::string& relpath);
};

struct SegmentWriteLock : public WriteLock
{
    SegmentWriteLock(const LocalConfig& config, const std::string& relpath);
};


}
}

#endif
