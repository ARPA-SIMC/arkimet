#include "file.h"
#include <algorithm>
#include <deque>
#include <cstring>
#include <sstream>

namespace arki {
namespace core {

Stdin::Stdin() : NamedFileDescriptor(0, "(stdin)") {}
Stdout::Stdout() : NamedFileDescriptor(1, "(stdout)") {}
Stderr::Stderr() : NamedFileDescriptor(2, "(stderr)") {}

namespace {

struct FDLineReader : public LineReader
{
    NamedFileDescriptor& fd;
    std::deque<char> linebuf;
    bool fd_eof = false;

    FDLineReader(NamedFileDescriptor& fd) : fd(fd) {}

    bool eof() const override { return fd_eof && linebuf.empty(); }

    bool getline(std::string& line) override
    {
        line.clear();
        while (true)
        {
            std::deque<char>::iterator i = std::find(linebuf.begin(), linebuf.end(), '\n');
            if (i != linebuf.end())
            {
                line.append(linebuf.begin(), i);
                linebuf.erase(linebuf.begin(), i + 1);
                return true;
            }

            if (fd_eof)
            {
                if (i == linebuf.end())
                    return false;

                line.append(linebuf.begin(), linebuf.end());
                linebuf.clear();
                return true;
            }

            // Read more and retry
            char buf[4096];
            size_t count = fd.read(buf, 4096);
            if (count == 0)
                fd_eof = true;
            else
                linebuf.insert(linebuf.end(), buf, buf + count);
        }
    }
};

struct StringLineReader : public LineReader
{
    const char* cur;
    const char* end;

    StringLineReader(const char* buf, size_t size)
        : cur(buf), end(buf + size) {}

    bool eof() const override { return cur == end; }

    bool getline(std::string& line) override
    {
        const char* pos = std::find(cur, end, '\n');
        if (pos == end)
        {
            if (cur == end)
            {
                line.clear();
                return false;
            }

            line.assign(cur, end);
            cur = end;
            return true;
        }

        line.assign(cur, pos);
        cur = pos + 1;
        return true;
    }
};

}


std::unique_ptr<LineReader> LineReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<LineReader>(new FDLineReader(fd));
}

std::unique_ptr<LineReader> LineReader::from_chars(const char* buf, size_t size)
{
    return std::unique_ptr<LineReader>(new StringLineReader(buf, size));
}


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
            msg << "lock is already held on " << fd.name() << " from ";
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
}
