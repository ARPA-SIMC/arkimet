#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki {
namespace dataset {
namespace local {
class Dataset;
}

class Lock : public core::Lock
{
public:
    arki::core::File lockfile;
    const core::lock::Policy* lock_policy;
    arki::core::FLock ds_lock;

    Lock(const std::filesystem::path& pathname, const core::lock::Policy* lock_policy);
};


class ReadLock : public Lock
{
public:
    ReadLock(const std::filesystem::path& pathname, const core::lock::Policy* lock_policy);
    ~ReadLock();
};


class AppendLock : public Lock
{
public:
    AppendLock(const std::filesystem::path& pathname, const core::lock::Policy* lock_policy);
    ~AppendLock();
};


class CheckLock : public Lock
{
public:
    std::weak_ptr<core::Lock> current_write_lock;

    CheckLock(const std::filesystem::path& pathname, const core::lock::Policy* lock_policy);
    ~CheckLock();

    /**
     * Escalate a read lock to a write lock as long as the resulting lock is in
     * use
     */
    std::shared_ptr<core::Lock> write_lock();
};


class DatasetReadLock : public ReadLock
{
public:
    explicit DatasetReadLock(const local::Dataset& dataset);
};

class DatasetAppendLock : public AppendLock
{
public:
    explicit DatasetAppendLock(const local::Dataset& dataset);
};

class DatasetCheckLock : public CheckLock
{
public:
    explicit DatasetCheckLock(const local::Dataset& dataset);
};

class SegmentReadLock : public ReadLock
{
public:
    SegmentReadLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};

class SegmentAppendLock : public AppendLock
{
public:
    SegmentAppendLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};

class SegmentCheckLock : public CheckLock
{
public:
    SegmentCheckLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};


}
}

#endif
