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
    ds_lock.l_len = 1;
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


AppendLock::AppendLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : Lock(pathname, lock_policy)
{
    trace("%s [%d] Requesting append lock\n", pathname.c_str(), (int)getpid());
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 1;
    ds_lock.l_len = 1;
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


CheckLock::CheckLock(const std::string& pathname, const core::lock::Policy* lock_policy)
    : Lock(pathname, lock_policy)
{
    trace("%s [%d] Requesting readonly check lock\n", pathname.c_str(), (int)getpid());
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 1;
    ds_lock.l_len = 1;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
    trace("%s [%d] Obtained readonly check lock\n", pathname.c_str(), (int)getpid());
}

CheckLock::~CheckLock()
{
    trace("%s [%d] Release check lock\n", lockfile.name().c_str(), (int)getpid());
    ds_lock.l_type = F_UNLCK;
    ds_lock.l_start = 0;
    ds_lock.l_len = 2;
    lock_policy->setlk(lockfile, ds_lock);
}

namespace {

struct TemporaryWriteLock : public core::Lock
{
    std::shared_ptr<CheckLock> parent;

    TemporaryWriteLock(std::shared_ptr<CheckLock> parent)
        : parent(parent)
    {
        trace("%s [%d] Requesting escalate to write check lock\n", parent->lockfile.name().c_str(), (int)getpid());
        parent->ds_lock.l_type = F_WRLCK;
        parent->ds_lock.l_start = 0;
        parent->ds_lock.l_len = 2;
        parent->lock_policy->setlkw(parent->lockfile, parent->ds_lock);
        trace("%s [%d] Obtained escalate to write check lock\n", parent->lockfile.name().c_str(), (int)getpid());
    }

    ~TemporaryWriteLock()
    {
        trace("%s [%d] Deescalate to readonly check lock\n", parent->lockfile.name().c_str(), (int)getpid());
        parent->ds_lock.l_type = F_UNLCK;
        parent->ds_lock.l_start = 0;
        parent->ds_lock.l_len = 1;
        parent->lock_policy->setlk(parent->lockfile, parent->ds_lock);
    }
};

}

std::shared_ptr<core::Lock> CheckLock::write_lock()
{
    if (!current_write_lock.expired())
        return current_write_lock.lock();

    std::shared_ptr<core::Lock> res(new TemporaryWriteLock(std::dynamic_pointer_cast<CheckLock>(this->shared_from_this())));
    current_write_lock = res;
    return res;
}

namespace {
const std::string& ensure_path(const std::string& pathname)
{
    sys::makedirs(str::dirname(pathname));
    return pathname;
}
}


DatasetReadLock::DatasetReadLock(const local::Dataset& dataset)
    : ReadLock(str::joinpath(dataset.path, "lock"), dataset.lock_policy)
{
}

SegmentReadLock::SegmentReadLock(const local::Dataset& dataset, const std::string& relpath)
    : ReadLock(str::joinpath(dataset.path, relpath + ".lock"), dataset.lock_policy)
{
}

DatasetAppendLock::DatasetAppendLock(const local::Dataset& dataset)
    : AppendLock(str::joinpath(dataset.path, "lock"), dataset.lock_policy)
{
}

SegmentAppendLock::SegmentAppendLock(const local::Dataset& dataset, const std::string& relpath)
    : AppendLock(str::joinpath(dataset.path, relpath + ".lock"), dataset.lock_policy)
{
}

DatasetCheckLock::DatasetCheckLock(const local::Dataset& dataset)
    : CheckLock(str::joinpath(dataset.path, "lock"), dataset.lock_policy)
{
}

SegmentCheckLock::SegmentCheckLock(const local::Dataset& dataset, const std::string& relpath)
    : CheckLock(ensure_path(str::joinpath(dataset.path, relpath + ".lock")), dataset.lock_policy)
{
}

}
}
