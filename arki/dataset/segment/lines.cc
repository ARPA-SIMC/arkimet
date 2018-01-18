#include "lines.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <arki/wibble/sys/signal.h>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace lines {

namespace {

struct File : public fd::File
{
    using fd::File::File;

    void write_data(off_t wrpos, const std::vector<uint8_t>& buf) override;
    void test_add_padding(size_t size) override;
};

void File::write_data(off_t wrpos, const std::vector<uint8_t>& buf)
{
    struct iovec todo[2] = {
        { (void*)buf.data(), buf.size() },
        { (void*)"\n", 1 },
    };

    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    lseek(wrpos);

    // Append the data
    ssize_t res = ::writev(*this, todo, 2);
    if (res < 0 || (unsigned)res != buf.size() + 1)
    {
        stringstream ss;
        ss << "cannot write " << (buf.size() + 1) << " bytes to " << name();
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    fdatasync();
}

void File::test_add_padding(size_t size)
{
    for (unsigned i = 0; i < size; ++i)
        write("\n", 1);
}

}



Writer::Writer(const std::string& root, const std::string& relname, const std::string& absname, int mode)
    : fd::Writer(root, relname, unique_ptr<fd::File>(new File(absname, O_WRONLY | O_CREAT | mode, 0666)))
{
}

void Checker::open()
{
    if (fd) return;
    fd = new File(absname, O_RDWR, 0666);
}

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 2, quick);
}

unique_ptr<fd::Writer> Checker::make_tmp_segment(const std::string& relname, const std::string& absname)
{
    return unique_ptr<fd::Writer>(new lines::Writer(root, relname, absname, O_TRUNC));
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    return fd::Checker::repack_impl(rootdir, mds, false, test_flags);
}

}
}
}
}
