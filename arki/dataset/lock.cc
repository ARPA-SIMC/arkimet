#include "lock.h"
#include "local.h"
#include "arki/utils/string.h"
#include <memory>

using namespace arki::utils;

namespace arki {
namespace dataset {

ReadLock::ReadLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : lockfile(pathname, O_RDWR | O_CREAT, 0777), lock_policy(lock_policy)
{
    ds_lock.l_type = F_RDLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
}

ReadLock::~ReadLock()
{
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}

namespace {

struct TemporaryWriteLock : public core::Lock
{
    std::shared_ptr<ReadLock> parent;

    TemporaryWriteLock(std::shared_ptr<ReadLock> parent)
        : parent(parent)
    {
        parent->ds_lock.l_type = F_WRLCK;
        parent->lock_policy->setlkw(parent->lockfile, parent->ds_lock);
    }

    ~TemporaryWriteLock()
    {
        parent->ds_lock.l_type = F_RDLCK;
        parent->lock_policy->setlkw(parent->lockfile, parent->ds_lock);
    }
};

}

std::shared_ptr<core::Lock> ReadLock::write_lock()
{
    if (!current_write_lock.expired())
        return current_write_lock.lock();

    std::shared_ptr<core::Lock> res(new TemporaryWriteLock(std::dynamic_pointer_cast<ReadLock>(this->shared_from_this())));
    current_write_lock = res;
    return res;
}


WriteLock::WriteLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : lockfile(pathname, O_RDWR | O_CREAT, 0777), lock_policy(lock_policy)
{
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
}

WriteLock::~WriteLock()
{
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}


DatasetReadLock::DatasetReadLock(const LocalConfig& config)
    : ReadLock(str::joinpath(config.path, "lock"), config.lock_policy)
{
}

SegmentReadLock::SegmentReadLock(const LocalConfig& config, const std::string& relpath)
    : ReadLock(str::joinpath(config.path, relpath, ".lock"), config.lock_policy)
{
}

DatasetWriteLock::DatasetWriteLock(const LocalConfig& config)
    : WriteLock(str::joinpath(config.path, "lock"), config.lock_policy)
{
}

SegmentWriteLock::SegmentWriteLock(const LocalConfig& config, const std::string& relpath)
    : WriteLock(str::joinpath(config.path, relpath, ".lock"), config.lock_policy)
{
}

}
}
