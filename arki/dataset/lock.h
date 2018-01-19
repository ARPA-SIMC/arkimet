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
    DatasetReadLock(const LocalConfig& config);
};

struct DatasetAppendLock : public AppendLock
{
    DatasetAppendLock(const LocalConfig& config);
};

struct DatasetCheckLock : public CheckLock
{
    DatasetCheckLock(const LocalConfig& config);
};

struct SegmentReadLock : public ReadLock
{
    SegmentReadLock(const LocalConfig& config, const std::string& relpath);
};

struct SegmentAppendLock : public AppendLock
{
    SegmentAppendLock(const LocalConfig& config, const std::string& relpath);
};

struct SegmentCheckLock : public CheckLock
{
    SegmentCheckLock(const LocalConfig& config, const std::string& relpath);
};


}
}

#endif
