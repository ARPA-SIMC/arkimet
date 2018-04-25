#include "lines.h"
#include "gzidx.h"
#include "arki/exceptions.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace lines {

namespace {

struct File : public fd::File
{
    using fd::File::File;

    size_t write_data(const std::vector<uint8_t>& buf) override
    {
        struct iovec todo[2] = {
            { (void*)buf.data(), buf.size() },
            { (void*)"\n", 1 },
        };

        // Prevent caching (ignore function result)
        //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

        // Append the data
        ssize_t res = ::writev(*this, todo, 2);
        if (res < 0 || (unsigned)res != buf.size() + 1)
        {
            stringstream ss;
            ss << "cannot write " << (buf.size() + 1) << " bytes to " << name();
            throw std::system_error(errno, std::system_category(), ss.str());
        }

        return buf.size() + 1;
    }
    void test_add_padding(size_t size) override
    {
        for (unsigned i = 0; i < size; ++i)
            write("\n", 1);
    }
};

}


const char* Segment::type() const { return "concat"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
{
    fd::Creator creator(rootdir, relpath, mds, std::unique_ptr<fd::File>(new File(abspath)));
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "vm2";
}


Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : fd::Reader(abspath, lock), m_segment(format, root, relpath, abspath)
{
}

const Segment& Reader::segment() const { return m_segment; }


Writer::Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(open_file(abspath, O_WRONLY | O_CREAT | mode, 0666)), m_segment(format, root, relpath, abspath)
{
}

const Segment& Writer::segment() const { return m_segment; }

std::unique_ptr<fd::File> Writer::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

Checker::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : m_segment(format, root, relpath, abspath)
{
}

const Segment& Checker::segment() const { return m_segment; }

std::unique_ptr<fd::File> Checker::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

std::unique_ptr<fd::File> Checker::open(const std::string& pathname)
{
    return std::unique_ptr<fd::File>(new File(pathname, O_RDWR, 0666));
}

namespace {

struct CheckBackend : public fd::CheckBackend
{
    using fd::CheckBackend::CheckBackend;

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + 1;
    }
};

}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(segment().abspath, segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

}
}
}
