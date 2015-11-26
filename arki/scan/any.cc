#include <arki/libconfig.h>
#include <arki/scan/any.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/types/source/blob.h>
#include <arki/utils/files.h>
#include <arki/utils/compress.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include <sstream>
#include <algorithm>
#include <utime.h>

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif
#ifdef HAVE_HDF5
#include <arki/scan/odimh5.h>
#endif
#ifdef HAVE_VM2
#include <arki/scan/vm2.h>
#endif
#ifdef HAVE_NETCDF
#include <arki/scan/nc.h>
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace scan {

static void scan_metadata(const std::string& file, metadata::Eater& c)
{
    //cerr << "Reading cached metadata from " << file << endl;
    Metadata::readFile(file, c);
}

static bool scan_file(const std::string& pathname, const std::string& basedir, const std::string& relname, const std::string& format, metadata::Eater& c)
{
    // Scan the file
    if (isCompressed(pathname))
        throw std::runtime_error("cannot scan " + relname + ".gz: file needs to be manually decompressed before scanning");

#ifdef HAVE_GRIBAPI
    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        scan::Grib scanner;
        scanner.open(pathname, basedir, relname);
        while (true)
        {
            unique_ptr<Metadata> md(new Metadata);
            if (!scanner.next(*md)) break;
            c.eat(move(md));
        }
        return true;
    }
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr") {
        scan::Bufr scanner;
        scanner.open(pathname, basedir, relname);
        while (true)
        {
            unique_ptr<Metadata> md(new Metadata);
            if (!scanner.next(*md)) break;
            c.eat(move(md));
        }
        return true;
    }
#endif
#ifdef HAVE_HDF5
    if ((format == "h5") || (format == "odim") || (format == "odimh5")) {
        scan::OdimH5 scanner;
        scanner.open(pathname, basedir, relname);
        while (true)
        {
            unique_ptr<Metadata> md(new Metadata);
            if (!scanner.next(*md)) break;
            c.eat(move(md));
        }
        return true;
    }
#endif
#ifdef HAVE_VM2
    if (format == "vm2") {
        scan::Vm2 scanner;
        scanner.open(pathname, basedir, relname);
        while (true)
        {
            unique_ptr<Metadata> md(new Metadata);
            if (!scanner.next(*md)) break;
            c.eat(move(md));
        }
        return true;
    }
#endif
#ifdef HAVE_NETCDF
    if (format == "nc") {
        scan::NetCDF scanner;
        scanner.open(pathname, basedir, relname);
        while (true)
        {
            unique_ptr<Metadata> md(new Metadata);
            if (!scanner.next(*md)) break;
            c.eat(move(md));
        }
        return true;
    }
#endif
	return false;
}

static bool dir_segment_fnames_lt(const std::string& a, const std::string& b)
{
    unsigned long na = strtoul(a.c_str(), 0, 10);
    unsigned long nb = strtoul(b.c_str(), 0, 10);
    return na < nb;
}

namespace {

struct DirSource : public metadata::Eater
{
   metadata::Eater& next;
   const std::string& dirname;
   size_t pos;

   DirSource(metadata::Eater& next, const std::string& dirname) : next(next), dirname(dirname), pos(0) {}

   void set_pos(size_t pos)
   {
       this->pos = pos;
   }

   bool eat(unique_ptr<Metadata>&& md) override
   {
       const source::Blob& i = md->sourceBlob();
       md->set_source(Source::createBlob(i.format, i.basedir, dirname, pos, i.size));
       return next.eat(move(md));
   }
};

}

static bool scan_dir(const std::string& pathname, const std::string& basedir, const std::string& relname, const std::string& format, metadata::Eater& c)
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
    DirSource dirsource(c, relname);
    for (vector<string>::const_iterator i = fnames.begin(); i != fnames.end(); ++i)
    {
        //cerr << "SCAN " << *i << " " << pathname << " " << basedir << " " << relname << endl;
        dirsource.set_pos(strtoul(i->c_str(), 0, 10));
        if (!scan_file(str::joinpath(pathname, *i), basedir, relname, format, dirsource))
            return false;

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

bool scan(const std::string& basedir, const std::string& relname, metadata::Eater& c)
{
    std::string format = guess_format(basedir, relname);

    // If we cannot detect a format, fail
    if (format.empty()) return false;
    return scan(basedir, relname, c, format);
}

bool scan(const std::string& basedir, const std::string& relname, metadata::Eater& c, const std::string& format)
{
    // If we scan standard input, assume uncompressed data and do not try to
    // look for an existing .metadata file
    if (relname == "-")
        return scan_file(relname, basedir, relname, format, c);

    // stat the file (or its compressed version)
    string pathname = str::joinpath(basedir, relname);
    unique_ptr<struct stat> st_file = sys::stat(pathname);
    if (!st_file.get())
        st_file = sys::stat(pathname + ".gz");
    if (!st_file.get())
        throw runtime_error(pathname + " or " + pathname + ".gz not found");

    // stat the metadata file, if it exists
    string md_pathname = pathname + ".metadata";
    unique_ptr<struct stat> st_md = sys::stat(md_pathname);

    if (st_md.get() and st_md->st_mtime >= st_file->st_mtime)
    {
        // If there is a usable metadata file, use it to save time
        scan_metadata(md_pathname, c);
        return true;
    } else if (S_ISDIR(st_file->st_mode)) {
        return scan_dir(pathname, basedir, relname, format, c);
    } else {
        return scan_file(pathname, basedir, relname, format, c);
    }
}

bool scan(const std::string& file, metadata::Eater& c)
{
    string basedir;
    string relname;
    utils::files::resolve_path(file, basedir, relname);
    return scan(basedir, relname, c);
}

bool scan(const std::string& file, metadata::Eater& c, const std::string& format)
{
    string basedir;
    string relname;
    utils::files::resolve_path(file, basedir, relname);
    return scan(basedir, relname, c, format);
}

bool canScan(const std::string& file)
{
	// Get the file extension
	size_t pos = file.rfind('.');
	if (pos == string::npos)
		// No extension, we do not know what it is
		return false;
	string ext = str::lower(file.substr(pos+1));

	// Check for known extensions
#ifdef HAVE_GRIBAPI
	if (ext == "grib" || ext == "grib1" || ext == "grib2")
		return true;
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr")
		return true;
#endif
#ifdef HAVE_HDF5
	if ((ext == "h5") || (ext == "odimh5") || (ext == "odim"))
		return true;
#endif
#ifdef HAVE_VM2
    if (ext == "vm2")
        return true;
#endif
#ifdef HAVE_NETCDF
    if (ext == "nc")
        return true;
#endif
	return false;
}

bool exists(const std::string& file)
{
    if (sys::exists(file)) return true;
    if (sys::exists(file + ".gz")) return true;
    return false;
}

bool isCompressed(const std::string& file)
{
    return !sys::exists(file) && sys::exists(file + ".gz");
}

time_t timestamp(const std::string& file)
{
    time_t res = sys::timestamp(file, 0);
    if (res != 0) return res;
    return sys::timestamp(file + ".gz", 0);
}

void compress(const std::string& file, size_t groupsize)
{
	utils::compress::DataCompressor compressor(file, groupsize);
	scan(file, compressor);
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

void Validator::validate(Metadata& md) const
{
    const auto& buf = md.getData();
    validate(buf.data(), buf.size());
}

const Validator& Validator::by_filename(const std::string& filename)
{
    // Get the file extension
    size_t pos = filename.rfind('.');
    if (pos == string::npos)
        // No extension, we do not know what it is
        throw runtime_error("cannot find a validator for " + filename + ": file name has no extension");
    string ext = str::lower(filename.substr(pos+1));

#ifdef HAVE_GRIBAPI
	if (ext == "grib" || ext == "grib1" || ext == "grib2")
		return grib::validator();
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr")
		return bufr::validator();
#endif
#ifdef HAVE_HDF5
	if ((ext == "h5") || (ext == "odimh5") || (ext == "odim"))
		return odimh5::validator();
#endif
#ifdef HAVE_VM2
   if (ext == "vm2")
       return vm2::validator();
#endif
#ifdef HAVE_NETCDF
   if (ext == "nc")
       return netcdf::validator();
#endif
    throw runtime_error("cannot find a validator for " + filename + ": no validator available");
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
