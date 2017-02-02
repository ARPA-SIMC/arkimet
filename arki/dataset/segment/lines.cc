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

Segment::Segment(const std::string& relname, const std::string& absname)
    : fd::Segment(relname, absname)
{
}

void Segment::test_add_padding(unsigned size)
{
    open();
    for (unsigned i = 0; i < size; ++i)
        fd.write("\n", 1);
}

void Segment::write(off_t wrpos, const std::vector<uint8_t>& buf)
{
    struct iovec todo[2] = {
        { (void*)buf.data(), buf.size() },
        { (void*)"\n", 1 },
    };

    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    fd.lseek(wrpos);

    // Append the data
    ssize_t res = ::writev(fd, todo, 2);
    if (res < 0 || (unsigned)res != buf.size() + 1)
    {
        stringstream ss;
        ss << "cannot write " << (buf.size() + 1) << " bytes to " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    if (fdatasync(fd) < 0)
        throw_file_error(absname, "cannot flush write");
}

State Segment::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 2, quick);
}

static fd::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<lines::Segment> res(new lines::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}

Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment, false, test_flags);
}

}
}
}
}
