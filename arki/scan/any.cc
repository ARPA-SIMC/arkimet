#include "arki/libconfig.h"
#include "arki/scan/any.h"
#include "arki/scan/base.h"
#include "arki/metadata.h"
#include "arki/segment.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
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

static bool scan_metadata(const std::string& pathname, const std::string& md_pathname, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = segment::Reader::for_pathname(utils::get_format(pathname), str::dirname(pathname), str::basename(pathname), pathname, lock);
    return Metadata::read_file(md_pathname, [&](unique_ptr<Metadata> md) {
        md->sourceBlob().lock(reader);
        return dest(move(md));
    });
}

static bool scan_file(
        const std::string& pathname,
        const std::string& basedir,
        const std::string& relpath,
        const std::string& format,
        std::shared_ptr<core::Lock> lock,
        metadata_dest_func dest)
{
    // Scan the file
    if (isCompressed(pathname))
        throw std::runtime_error("cannot scan " + relpath + ".gz: file needs to be manually decompressed before scanning");

    auto st = sys::stat(pathname);
    if (!st)
        throw std::runtime_error("cannot scan " + relpath + ": file does not exist");

    auto scanner = scan::Scanner::get_scanner(format);
    scanner->open(pathname, basedir, relpath, lock);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!scanner->next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

static bool dir_segment_fnames_lt(const std::string& a, const std::string& b)
{
    unsigned long na = strtoul(a.c_str(), 0, 10);
    unsigned long nb = strtoul(b.c_str(), 0, 10);
    return na < nb;
}

static bool scan_dir(const std::string& pathname, const std::string& basedir, const std::string& relpath, const std::string& format, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    // Collect all file names in the directory
    vector<std::string> fnames;
    sys::Path dir(pathname);
    for (sys::Path::iterator di = dir.begin(); di != dir.end(); ++di)
        if (di.isreg() && str::endswith(di->d_name, format))
            fnames.push_back(di->d_name);

    // Sort them numerically
    std::sort(fnames.begin(), fnames.end(), dir_segment_fnames_lt);

    // Scan them one by one
    size_t pos = 0;
    for (vector<string>::const_iterator i = fnames.begin(); i != fnames.end(); ++i)
    {
        //cerr << "SCAN " << *i << " " << pathname << " " << basedir << " " << relpath << endl;
        pos = strtoul(i->c_str(), 0, 10);
        scan_file(str::joinpath(pathname, *i), basedir, relpath, format, lock, [&](unique_ptr<Metadata> md) {
           const source::Blob& i = md->sourceBlob();
           md->set_source(Source::createBlob(i.format, i.basedir, relpath, pos, i.size, i.reader));
           return dest(move(md));
        });

    }

    return true;
}

static std::string guess_format(const std::string& basedir, const std::string& file)
{
    // Get the file extension
    size_t pos = file.rfind('.');
    if (pos == string::npos)
        // No extension, we do not know what it is
        return std::string();
    return str::lower(file.substr(pos+1));
}

bool scan(const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    std::string format = guess_format(basedir, relpath);

    // If we cannot detect a format, fail
    if (format.empty())
        throw std::runtime_error("unknown format: '" + format + "'");
    return scan(basedir, relpath, lock, format, dest);
}

bool scan(const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock, const std::string& format, metadata_dest_func dest)
{
    // If we scan standard input, assume uncompressed data and do not try to
    // look for an existing .metadata file
    if (relpath == "-")
        return scan_file(relpath, basedir, relpath, format, lock, dest);

    // stat the file (or its compressed version)
    string pathname = str::joinpath(basedir, relpath);
    unique_ptr<struct stat> st_file = sys::stat(pathname);
    if (!st_file.get())
        st_file = sys::stat(pathname + ".tar");
    if (!st_file.get())
        st_file = sys::stat(pathname + ".zip");
    if (!st_file.get())
        st_file = sys::stat(pathname + ".gz");
    if (!st_file.get())
        throw runtime_error(pathname + " or " + pathname + ".gz or " + pathname + ".tar or " + pathname = ".zip not found");

    // stat the metadata file, if it exists
    string md_pathname = pathname + ".metadata";
    unique_ptr<struct stat> st_md = sys::stat(md_pathname);

    if (st_md.get() and st_md->st_mtime >= st_file->st_mtime)
    {
        // If there is a usable metadata file, use it to save time
        return scan_metadata(pathname, md_pathname, lock, dest);
    } else if (S_ISDIR(st_file->st_mode)) {
        return scan_dir(pathname, basedir, relpath, format, lock, dest);
    } else {
        return scan_file(pathname, basedir, relpath, format, lock, dest);
    }
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

void compress(const std::string& file, std::shared_ptr<core::Lock> lock, size_t groupsize)
{
    utils::compress::DataCompressor compressor(file, groupsize);
    scan(file, lock, [&](unique_ptr<Metadata> md) {
        return compressor.eat(move(md));
    });
    compressor.flush();

    // Set the same timestamp as the uncompressed file
    std::unique_ptr<struct stat> st = sys::stat(file);
    struct utimbuf times;
    times.actime = st->st_atime;
    times.modtime = st->st_mtime;
    utime((file + ".gz").c_str(), &times);
    utime((file + ".gz.idx").c_str(), &times);

	// TODO: delete uncompressed version
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
