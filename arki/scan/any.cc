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

static std::string guess_format(const std::string& basedir, const std::string& file)
{
    // Get the file extension
    size_t pos = file.rfind('.');
    if (pos == string::npos)
        // No extension, we do not know what it is
        return std::string();
    return str::lower(file.substr(pos+1));
}

static bool scan(const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock, const std::string& format, metadata_dest_func dest)
{
    string pathname = str::joinpath(basedir, relpath);
    auto reader = segment::Reader::for_pathname(format, basedir, relpath, pathname, lock);
    return reader->scan(dest);
}

static bool scan(const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    std::string format = guess_format(basedir, relpath);

    // If we cannot detect a format, fail
    if (format.empty())
        throw std::runtime_error("unknown format: '" + format + "'");
    return scan(basedir, relpath, lock, format, dest);
}

bool scan(const std::string& file, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    string basedir;
    string relpath;
    utils::files::resolve_path(file, basedir, relpath);
    return scan(basedir, relpath, lock, dest);
}

bool scan(const std::string& file, std::shared_ptr<core::Lock> lock, const std::string& format, metadata_dest_func dest)
{
    string basedir;
    string relpath;
    utils::files::resolve_path(file, basedir, relpath);
    return scan(basedir, relpath, lock, format, dest);
}

bool exists(const std::string& file)
{
    if (sys::exists(file)) return true;
    if (sys::exists(file + ".gz")) return true;
    return false;
}

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

void Validator::throw_check_error(utils::sys::NamedFileDescriptor& fd, off_t offset, const std::string& msg) const
{
    stringstream ss;
    ss << fd.name() << ":" << offset << ": " << format() << " validation failed: " << msg;
    throw runtime_error(ss.str());
}

void Validator::throw_check_error(const std::string& msg) const
{
    stringstream ss;
    ss << format() << " validation failed: " << msg;
    throw runtime_error(ss.str());
}

const Validator& Validator::by_filename(const std::string& filename)
{
    return by_format(get_format(filename));
}

const Validator& Validator::by_format(const std::string& format)
{
    return scan::Scanner::get_validator(format);
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
