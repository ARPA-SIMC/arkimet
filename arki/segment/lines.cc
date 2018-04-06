#include "lines.h"
#include "gzidx.h"
#include "arki/exceptions.h"
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



Writer::Writer(const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(root, relpath, open_file(abspath, O_WRONLY | O_CREAT | mode, 0666))
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

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 2, quick);
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    return fd::Checker::repack_impl(rootdir, mds, false, test_flags);
}

std::shared_ptr<segment::Checker> Checker::compress(metadata::Collection& mds)
{
    segment::gzidxlines::Checker::create(root, relpath + ".tar", abspath + ".tar", mds);
    remove();
    return make_shared<segment::gzidxlines::Checker>(root, relpath, abspath);
}

}
}
}
