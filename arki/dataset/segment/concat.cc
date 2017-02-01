#include "concat.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <arki/wibble/sys/signal.h>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace concat {

Segment::Segment(const std::string& relname, const std::string& absname)
    : fd::Segment(relname, absname)
{
}

void Segment::test_add_padding(unsigned size)
{
    open();
    for (unsigned i = 0; i < size; ++i)
        fd.write("", 1);
}

void HoleSegment::write(off_t wrpos, const std::vector<uint8_t>& buf)
{
    // Get the current file size
    off_t size = fd.lseek(0, SEEK_END);

    if (wrpos + buf.size() <= (size_t)size)
        return;

    // Enlarge its apparent size to include the size of buf
    int res = ftruncate(fd, wrpos + buf.size());
    if (res < 0)
    {
        stringstream msg;
        msg << absname << ": cannot set apparent segment size to " << (wrpos + buf.size()) << " bytes";
        throw std::system_error(errno, std::system_category(), msg.str());
    }
}

State Segment::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 0, quick);
}

static fd::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment, false, test_flags);
}

static fd::Segment* make_repack_hole_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::HoleSegment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending HoleSegment::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_hole_segment, true, test_flags);
}

}
}
}
}
