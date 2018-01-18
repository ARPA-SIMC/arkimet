#include "lock.h"
#include "local.h"
#include "arki/utils/string.h"
#include <memory>

//#define TRACE_LOCKS
#ifdef TRACE_LOCKS
#define trace(...) fprintf(stderr, __VA_ARGS__)
#else
#define trace(...) do {} while(0)
#endif

using namespace arki::utils;

namespace arki {
namespace dataset {

Lock::Lock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : lockfile(pathname, O_RDWR | O_CREAT, 0777), lock_policy(lock_policy)
{
}

ReadLock::ReadLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : Lock(pathname, lock_policy)
{
    trace("%s [%d] Requesting read lock\n", pathname.c_str(), (int)getpid());
    ds_lock.l_type = F_RDLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
    trace("%s [%d] Obtained read lock\n", pathname.c_str(), (int)getpid());
}

ReadLock::~ReadLock()
{
    trace("%s [%d] Release read lock\n", lockfile.name().c_str(), (int)getpid());
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
        trace("%s [%d] Requesting escalate to write lock\n", parent->lockfile.name().c_str(), (int)getpid());
        parent->ds_lock.l_type = F_WRLCK;
        parent->lock_policy->setlkw(parent->lockfile, parent->ds_lock);
        trace("%s [%d] Obtained escalate to write lock\n", parent->lockfile.name().c_str(), (int)getpid());
    }

    ~TemporaryWriteLock()
    {
        trace("%s [%d] Deescalate to read lock\n", parent->lockfile.name().c_str(), (int)getpid());
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
    : Lock(pathname, lock_policy)
{
    trace("%s [%d] Requesting write lock\n", pathname.c_str(), (int)getpid());
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
    trace("%s [%d] Obtained write lock\n", pathname.c_str(), (int)getpid());
}

WriteLock::~WriteLock()
{
    trace("%s [%d] Release write lock\n", lockfile.name().c_str(), (int)getpid());
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}

std::shared_ptr<core::Lock> WriteLock::write_lock()
{
    return shared_from_this();
}


AppendLock::AppendLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : Lock(pathname, lock_policy)
{
    trace("%s [%d] Requesting append lock\n", pathname.c_str(), (int)getpid());
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
    trace("%s [%d] Obtained append lock\n", pathname.c_str(), (int)getpid());
}

AppendLock::~AppendLock()
{
    trace("%s [%d] Release append lock\n", lockfile.name().c_str(), (int)getpid());
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}

std::shared_ptr<core::Lock> AppendLock::write_lock()
{
    return shared_from_this();
}


DatasetReadLock::DatasetReadLock(const LocalConfig& config)
    : ReadLock(str::joinpath(config.path, "lock"), config.lock_policy)
{
}

SegmentReadLock::SegmentReadLock(const LocalConfig& config, const std::string& relpath)
    : ReadLock(str::joinpath(config.path, relpath + ".lock"), config.lock_policy)
{
}

DatasetWriteLock::DatasetWriteLock(const LocalConfig& config)
    : WriteLock(str::joinpath(config.path, "lock"), config.lock_policy)
{
}

SegmentWriteLock::SegmentWriteLock(const LocalConfig& config, const std::string& relpath)
    : WriteLock(str::joinpath(config.path, relpath + ".lock"), config.lock_policy)
{
}

DatasetAppendLock::DatasetAppendLock(const LocalConfig& config)
    : AppendLock(str::joinpath(config.path, "lock"), config.lock_policy)
{
}

SegmentAppendLock::SegmentAppendLock(const LocalConfig& config, const std::string& relpath)
    : AppendLock(str::joinpath(config.path, relpath + ".lock"), config.lock_policy)
{
}

}
}
