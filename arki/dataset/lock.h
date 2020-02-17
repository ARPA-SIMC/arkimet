#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki {
namespace dataset {
namespace local {
struct Dataset;
}

struct Lock : public core::Lock
{
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;

    Lock(const std::string& pathname, const core::lock::Policy* lock_policy);
};


struct ReadLock : public Lock
{
    ReadLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~ReadLock();
};


struct AppendLock : public Lock
{
    AppendLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~AppendLock();
};


struct CheckLock : public Lock
{
    std::weak_ptr<core::Lock> current_write_lock;

    CheckLock(const std::string& pathname, const core::lock::Policy* lock_policy);
    ~CheckLock();

    /**
     * Escalate a read lock to a write lock as long as the resulting lock is in
     * use
     */
    std::shared_ptr<core::Lock> write_lock();
};


struct DatasetReadLock : public ReadLock
{
    DatasetReadLock(const local::Dataset& dataset);
};

struct DatasetAppendLock : public AppendLock
{
    DatasetAppendLock(const local::Dataset& dataset);
};

struct DatasetCheckLock : public CheckLock
{
    DatasetCheckLock(const local::Dataset& dataset);
};

struct SegmentReadLock : public ReadLock
{
    SegmentReadLock(const local::Dataset& dataset, const std::string& relpath);
};

struct SegmentAppendLock : public AppendLock
{
    SegmentAppendLock(const local::Dataset& dataset, const std::string& relpath);
};

struct SegmentCheckLock : public CheckLock
{
    SegmentCheckLock(const local::Dataset& dataset, const std::string& relpath);
};


}
}

#endif
