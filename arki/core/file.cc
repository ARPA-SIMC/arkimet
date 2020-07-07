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

AbstractInputFile::~AbstractInputFile() {}
AbstractOutputFile::~AbstractOutputFile() {}

BufferOutputFile::BufferOutputFile(std::vector<uint8_t>& buffer, const std::string& name)
    : buffer(buffer), m_name(name)
{
}

std::string BufferOutputFile::name() const { return m_name; }
void BufferOutputFile::write(const void* data, size_t size)
{
    buffer.insert(buffer.end(), static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + size);
}


/*
 * BufferedReader
 */

BufferedReader::~BufferedReader()
{
}

bool BufferedReader::refill()
{
    pos = 0;
    end = read();
    if (end == 0)
        return false;
    return true;
}

int BufferedReader::peek()
{
    if (pos >= end)
        if (!refill())
            return EOF;
    return buffer[pos];
}

int BufferedReader::get()
{
    if (pos >= end)
        if (!refill())
            return EOF;
    return buffer[pos++];
}


namespace {

template<typename Input>
struct FDBufferedReader : public BufferedReader
{
    Input& fd;

    FDBufferedReader(Input& fd) : fd(fd) {}

    unsigned read() override
    {
        return fd.read(buffer.data(), buffer.size());
    }
};

struct StringBufferedReader : public BufferedReader
{
    const char* cur;
    const char* end;

    StringBufferedReader(const char* buf, size_t size)
        : cur(buf), end(buf + size) {}

    unsigned read() override
    {
        if (cur == end)
            return 0;
        unsigned size = buffer.size();
        if (end - cur < size)
            size = end - cur;
        memcpy(buffer.data(), cur, size);
        cur += size;
        return size;
    }
};

template<typename Buffer>
struct LineReaderImpl : public LineReader, Buffer
{
    using Buffer::Buffer;

    bool getline(std::string& line) override
    {
        line.clear();
        while (true)
        {
            int c = this->get();
            if (c == EOF)
            {
                this->fd_eof = true;
                return false;
            }

            if (c == '\n')
                return true;

            line += (char)c;
        }
    }
};

}

std::unique_ptr<BufferedReader> BufferedReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<BufferedReader>(new FDBufferedReader<NamedFileDescriptor>(fd));
}

std::unique_ptr<BufferedReader> BufferedReader::from_abstract(AbstractInputFile& fd)
{
    return std::unique_ptr<BufferedReader>(new FDBufferedReader<AbstractInputFile>(fd));
}

std::unique_ptr<BufferedReader> BufferedReader::from_chars(const char* buf, size_t size)
{
    return std::unique_ptr<BufferedReader>(new StringBufferedReader(buf, size));
}

std::unique_ptr<BufferedReader> BufferedReader::from_string(const std::string& str)
{
    return std::unique_ptr<BufferedReader>(new StringBufferedReader(str.data(), str.size()));
}


std::unique_ptr<LineReader> LineReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<LineReader>(new LineReaderImpl<FDBufferedReader<NamedFileDescriptor>>(fd));
}

std::unique_ptr<LineReader> LineReader::from_abstract(AbstractInputFile& fd)
{
    return std::unique_ptr<LineReader>(new LineReaderImpl<FDBufferedReader<AbstractInputFile>>(fd));
}

std::unique_ptr<LineReader> LineReader::from_chars(const char* buf, size_t size)
{
    return std::unique_ptr<LineReader>(new LineReaderImpl<StringBufferedReader>(buf, size));
}

std::unique_ptr<LineReader> LineReader::from_string(const std::string& str)
{
    return std::unique_ptr<LineReader>(new LineReaderImpl<StringBufferedReader>(str.data(), str.size()));
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
