#include "lock.h"
#include "local.h"
#include "arki/utils/string.h"

using namespace arki::utils;

namespace arki {
namespace dataset {

DatasetReadLock::DatasetReadLock(const LocalConfig& config)
    : lockfile(str::joinpath(config.path, "lock"), O_RDWR | O_CREAT, 0777), lock_policy(config.lock_policy)
{
    ds_lock.l_type = F_RDLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
}

DatasetReadLock::~DatasetReadLock()
{
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}

namespace {

struct TemporaryWriteLock : public core::Lock
{
    DatasetReadLock& parent;

    TemporaryWriteLock(DatasetReadLock& parent)
        : parent(parent)
    {
        parent.ds_lock.l_type = F_WRLCK;
        parent.lock_policy->setlkw(parent.lockfile, parent.ds_lock);
    }

    ~TemporaryWriteLock()
    {
        parent.ds_lock.l_type = F_RDLCK;
        parent.lock_policy->setlkw(parent.lockfile, parent.ds_lock);
    }
};

}

std::shared_ptr<core::Lock> DatasetReadLock::write_lock()
{
    if (!current_write_lock.expired())
        return current_write_lock.lock();

    std::shared_ptr<core::Lock> res(new TemporaryWriteLock(*this));
    current_write_lock = res;
    return res;
}


DatasetWriteLock::DatasetWriteLock(const LocalConfig& config)
    : lockfile(str::joinpath(config.path, "lock"), O_RDWR | O_CREAT, 0777), lock_policy(config.lock_policy)
{
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    lock_policy->setlkw(lockfile, ds_lock);
}

DatasetWriteLock::~DatasetWriteLock()
{
    ds_lock.l_type = F_UNLCK;
    lock_policy->setlk(lockfile, ds_lock);
}

}
}
