#include "scan.h"
#include "arki/libconfig.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/core/file.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/files.h"
#ifdef HAVE_GRIBAPI
#include "arki/scan/grib.h"
#endif
#ifdef HAVE_DBALLE
#include "arki/scan/bufr.h"
#endif
#include "arki/scan/odimh5.h"
#include "arki/scan/netcdf.h"
#include "arki/scan/jpeg.h"
#ifdef HAVE_VM2
#include "arki/scan/vm2.h"
#endif
#include <unordered_map>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

typedef std::function<std::shared_ptr<Scanner>()> factory;

static std::unordered_map<std::string, factory> factories;

static std::unordered_map<std::string, std::shared_ptr<Scanner>> scanner_cache;

void init()
{
    factories["grib"] = [] {
        return std::make_shared<scan::MockGribScanner>();
    };
#ifdef HAVE_DBALLE
    factories["bufr"] = [] {
        return std::make_shared<scan::MockBufrScanner>();
    };
#endif

    register_odimh5_scanner();
    register_netcdf_scanner();
    register_jpeg_scanner();

#ifdef HAVE_VM2
    factories["vm2"] = [] {
        return std::make_shared<scan::Vm2>();
    };
#endif
}

Scanner::~Scanner()
{
}

void Scanner::register_factory(const std::string& name, std::function<std::shared_ptr<Scanner>()> factory)
{
    factories[name] = factory;
    // Clear scanner cache, since from now on the scanners we should create
    // might have changed
    scanner_cache.clear();
}

bool Scanner::test_scan_file(const std::filesystem::path& filename, metadata_dest_func dest)
{
    std::filesystem::path basedir, relpath;
    utils::files::resolve_path(filename, basedir, relpath);
    return scan_segment(Segment::detect_reader(format_from_filename(filename), basedir, relpath, filename, make_shared<core::lock::Null>()), dest);
}

void Scanner::normalize_before_dispatch(Metadata& md)
{
}

std::shared_ptr<Scanner> Scanner::get_scanner(const std::string& format)
{
    // Lookup in cache first, before normalisation
    auto cached = scanner_cache.find(format);
    if (cached != scanner_cache.end())
        return cached->second;

    // Normalise format and look up again
    auto normalised_format = normalise_format(format);
    cached = scanner_cache.find(format);
    if (cached != scanner_cache.end())
    {
        scanner_cache[format] = cached->second;
        return cached->second;
    }

    // Instantiate
    auto i = factories.find(normalised_format);
    if (i == factories.end())
        throw std::runtime_error("No scanner available for format '" + format + "'");
    auto res = i->second();
    scanner_cache[format] = res;
    return res;
}

const Validator& Scanner::get_validator(const std::string& format)
{
#ifdef HAVE_GRIBAPI
    if (format == "grib")
        return grib::validator();
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return bufr::validator();
#endif

    if (format == "odimh5")
        return odimh5::validator();

    if (format == "nc")
        return netcdf::validator();

    if (format == "jpeg")
        return jpeg::validator();

#ifdef HAVE_VM2
   if (format == "vm2")
       return vm2::validator();
#endif
    throw std::runtime_error("No validator available for format '" + format + "'");
}

std::string Scanner::normalise_format(const std::string& format, const char* default_format)
{
    std::string f = str::lower(format);
    if (f == "grib") return "grib";
    if (f == "grib1") return "grib";
    if (f == "grib2") return "grib";

    if (f == "bufr") return "bufr";
    if (f == "vm2") return "vm2";

    if (f == "h5")     return "odimh5";
    if (f == "hdf5")   return "odimh5";
    if (f == "odim")   return "odimh5";
    if (f == "odimh5") return "odimh5";

    if (f == "nc") return "nc";
    if (f == "netcdf") return "nc";

    if (f == "jpg") return "jpeg";
    if (f == "jpeg") return "jpeg";

    if (f == "yaml") return "yaml";
    if (f == "arkimet") return "arkimet";
    if (f == "metadata") return "arkimet";
    if (default_format) return default_format;
    throw std::runtime_error("unsupported format `" + format + "`");
}

std::string Scanner::format_from_filename(const std::filesystem::path& fname, const char* default_format)
{
    // Extract the extension
    auto ext = fname.extension();
    if (not ext.empty())
    {
        if (ext == ".zip" || ext == ".gz" || ext == ".tar")
            ext = fname.stem().extension();
    }

    if (not ext.empty())
        return normalise_format(str::lower(ext.native().substr(1)), default_format);
    else if (default_format)
        return default_format;
    else
    {
        stringstream ss;
        ss << "cannot auto-detect format from file name " << fname << ": file extension not recognised";
        throw std::runtime_error(ss.str());
    }
}

bool Scanner::update_sequence_number(const types::source::Blob& source, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (source.format != "bufr")
        return false;

    auto data = source.read_data();
    string buf((const char*)data.data(), data.size());
    usn = BufrScanner::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

bool Scanner::update_sequence_number(Metadata& md, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (md.source().format != "bufr")
        return false;

    const auto& data = md.get_data();
    auto buf = data.read();
    string strbuf((const char*)buf.data(), buf.size());
    usn = BufrScanner::update_sequence_number(strbuf);
    return true;
#else
    return false;
#endif
}

std::vector<uint8_t> Scanner::reconstruct(const std::string& format, const Metadata& md, const std::string& value)
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
