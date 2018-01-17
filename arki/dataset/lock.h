#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki {
namespace dataset {
struct LocalConfig;

struct ReadLock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;
    std::weak_ptr<core::Lock> current_write_lock;

    ReadLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~ReadLock();

    /**
     * Escalate a read lock to a write lock as long as the resulting lock is in
     * use
     */
    std::shared_ptr<core::Lock> write_lock();
};


struct WriteLock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;

    WriteLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~WriteLock();
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
