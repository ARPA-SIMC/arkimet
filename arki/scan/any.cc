#include "arki/libconfig.h"
#include "arki/scan/any.h"
#include "arki/scan/base.h"
#include "arki/metadata.h"
#include "arki/segment.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <sstream>
#include <algorithm>
#include <utime.h>
#include <fcntl.h>

#ifdef HAVE_DBALLE
#include "arki/scan/bufr.h"
#endif
#ifdef HAVE_VM2
#include "arki/scan/vm2.h"
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace scan {

bool isCompressed(const std::string& file)
{
    return !sys::exists(file) && (sys::exists(file + ".gz") || sys::exists(file + ".tar") || sys::exists(file + ".zip"));
}

time_t timestamp(const std::string& file)
{
    auto st = sys::stat(file);
    if (!st)
    {
        // File does not exist, it might have a compressed version
        st = sys::stat(file + ".gz");
        if (!st) return 0;
        return st->st_mtime;
    }

    if (!S_ISDIR(st->st_mode))
        return st->st_mtime;

    // Directory segment, check the timestamp of the sequence file instead
    return sys::timestamp(str::joinpath(file, ".sequence"), 0);
}

bool update_sequence_number(const types::source::Blob& source, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (source.format != "bufr")
        return false;

    auto data = source.read_data();
    string buf((const char*)data.data(), data.size());
    usn = Bufr::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

bool update_sequence_number(Metadata& md, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (md.source().format != "bufr")
        return false;

    const auto& data = md.getData();
    string buf((const char*)data.data(), data.size());
    usn = Bufr::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

std::vector<uint8_t> reconstruct(const std::string& format, const Metadata& md, const std::string& value)
{
#ifdef HAVE_VM2
    if (format == "vm2")
    {
        return scan::Vm2::reconstruct(md, value);
    }
#endif
    throw runtime_error("cannot reconstruct " + format + " data: format not supported");
}

}
}
