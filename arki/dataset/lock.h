#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki {
namespace dataset {
struct LocalConfig;

struct DatasetReadLock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;
    std::weak_ptr<core::Lock> current_write_lock;

    DatasetReadLock(const LocalConfig& config);
    ~DatasetReadLock();

    /**
     * Escalate a read lock to a write lock as long as the resulting lock is in
     * use
     */
    std::shared_ptr<core::Lock> write_lock();
};

struct DatasetWriteLock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;

    DatasetWriteLock(const LocalConfig& config);
    ~DatasetWriteLock();
};

}
}

#endif
