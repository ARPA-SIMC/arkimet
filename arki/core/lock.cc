#include "lock.h"
#include <cstring>
#include <cstdio>
#include <limits>
#include <sstream>

namespace arki::core {

namespace lock {

static bool test_nowait = false;
static unsigned count_ofd_setlk = 0;
static unsigned count_ofd_setlkw = 0;
static unsigned count_ofd_getlk = 0;

}

Lock::~Lock() {}


FLock::FLock()
{
    memset(this, 0, sizeof(struct ::flock));
}

bool FLock::ofd_setlk(NamedFileDescriptor& fd)
{
    ++lock::count_ofd_setlk;
    return fd.ofd_setlk(*this);
}

bool FLock::ofd_setlkw(NamedFileDescriptor& fd, bool retry_on_signal)
{
    ++lock::count_ofd_setlkw;
    if (lock::test_nowait)
    {
        // Try to inquire about the lock, to maybe get information on what has
        // it held
        struct ::flock l = *this;
        if (!fd.ofd_getlk(l))
        {
            std::stringstream msg;
            msg << "a ";
            if (l.l_type == F_RDLCK)
                msg << "read ";
            else
                msg << "write ";
            msg << "lock is already held on " << fd.path() << " from ";
            switch (l.l_whence)
            {
                case SEEK_SET: msg << "set:"; break;
                case SEEK_CUR: msg << "cur:"; break;
                case SEEK_END: msg << "end:"; break;
            }
            msg << l.l_start << " len: " << l.l_len;
            throw std::runtime_error(msg.str());
        }

        // Then try and actually obtain it, raising an exception if it fails
        if (!fd.ofd_setlk(*this))
            throw std::runtime_error("file already locked");

        return true;
    }
    else
        return fd.ofd_setlkw(*this, retry_on_signal);
}

bool FLock::ofd_getlk(NamedFileDescriptor& fd)
{
    ++lock::count_ofd_getlk;
    return fd.ofd_getlk(*this);
}


namespace lock {

TestWait::TestWait()
    : orig(test_nowait)
{
    test_nowait = false;
}

TestWait::~TestWait()
{
    test_nowait = orig;
}

TestNowait::TestNowait()
    : orig(test_nowait)
{
    test_nowait = true;
}

TestNowait::~TestNowait()
{
    test_nowait = orig;
}

void test_set_nowait_default(bool value)
{
    test_nowait = value;
}


TestCount::TestCount()
    : initial_ofd_setlk(count_ofd_setlk),
      initial_ofd_setlkw(count_ofd_setlkw),
      initial_ofd_getlk(count_ofd_getlk)
{
}

static unsigned count_diff(unsigned initial, unsigned current) noexcept
{
    if (initial > current)
        // Counter wrapped around
        return std::numeric_limits<unsigned>::max() - initial + current;
    else
        return current - initial;
}

void TestCount::measure()
{
    ofd_setlk = count_diff(initial_ofd_setlk, count_ofd_setlk);
    ofd_setlkw = count_diff(initial_ofd_setlkw, count_ofd_setlkw);
    ofd_getlk = count_diff(initial_ofd_getlk, count_ofd_getlk);
}


Policy::~Policy() {}

/// Lock Policy that does nothing
struct NullPolicy : public Policy
{
    bool setlk(NamedFileDescriptor& fd, FLock&) const override;
    bool setlkw(NamedFileDescriptor& fd, FLock&) const override;
    bool getlk(NamedFileDescriptor& fd, FLock&) const override;
};

/// Lock Policy that uses Open File Descriptor locks
struct OFDPolicy : public Policy
{
    bool setlk(NamedFileDescriptor& fd, FLock&) const override;
    bool setlkw(NamedFileDescriptor& fd, FLock&) const override;
    bool getlk(NamedFileDescriptor& fd, FLock&) const override;
};


bool NullPolicy::setlk(NamedFileDescriptor& fd, FLock&) const { return true; }
bool NullPolicy::setlkw(NamedFileDescriptor& fd, FLock&) const { return true; }
bool NullPolicy::getlk(NamedFileDescriptor& fd, FLock&) const { return true; }

bool OFDPolicy::setlk(NamedFileDescriptor& fd, FLock& l) const { return l.ofd_setlk(fd); }
bool OFDPolicy::setlkw(NamedFileDescriptor& fd, FLock& l) const { return l.ofd_setlkw(fd); }
bool OFDPolicy::getlk(NamedFileDescriptor& fd, FLock& l) const { return l.ofd_getlk(fd); }

const Policy* policy_null = new NullPolicy;
const Policy* policy_ofd = new OFDPolicy;

}

}
