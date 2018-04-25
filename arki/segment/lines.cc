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


Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : fd::Reader(abspath, lock), m_segment(format, root, relpath, abspath)
{
}

const Segment& Reader::segment() const { return m_segment; }


Writer::Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(format, root, relpath, open_file(abspath, O_WRONLY | O_CREAT | mode, 0666))
{
}

const char* Writer::type() const { return "lines"; }

std::unique_ptr<fd::File> Writer::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

const char* Checker::type() const { return "lines"; }

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

std::shared_ptr<segment::Reader> Checker::reader(std::shared_ptr<core::Lock> lock)
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(abspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

bool Checker::can_store(const std::string& format)
{
    return format == "vm2";
}

std::shared_ptr<Checker> Checker::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
{
    fd::Creator creator(rootdir, relpath, mds, std::unique_ptr<fd::File>(new File(abspath)));
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}

}
}
}
