#include "scan.h"
#include "arki/libconfig.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/core/file.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/files.h"
#ifdef HAVE_GRIBAPI
#include "arki/scan/grib.h"
#endif
#ifdef HAVE_DBALLE
#include "arki/scan/bufr.h"
#endif
#include "arki/scan/odimh5.h"
#ifdef HAVE_VM2
#include "arki/scan/vm2.h"
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

typedef std::function<std::unique_ptr<Scanner>()> factory;

static std::map<std::string, factory> factories;

void init()
{
    factories["grib"] = [] {
        return std::unique_ptr<Scanner>(new scan::MockGribScanner);
    };
#ifdef HAVE_DBALLE
    factories["bufr"] = [] {
        return std::unique_ptr<Scanner>(new scan::MockBufrScanner);
    };
#endif

    register_odimh5_lua();

#ifdef HAVE_VM2
    factories["vm2"] = [] {
        return std::unique_ptr<Scanner>(new scan::Vm2);
    };
#endif
}

Scanner::~Scanner()
{
}

void Scanner::register_factory(const std::string& name, std::function<std::unique_ptr<Scanner>()> factory)
{
    factories[name] = factory;
}

bool Scanner::test_scan_file(const std::string& filename, metadata_dest_func dest)
{
    string basedir, relpath;
    utils::files::resolve_path(filename, basedir, relpath);
    return scan_segment(Segment::detect_reader(format_from_filename(filename), basedir, relpath, filename, make_shared<core::lock::Null>()), dest);
}

std::unique_ptr<Scanner> Scanner::get_scanner(const std::string& format)
{
    auto i = factories.find(normalise_format(format));
    if (i == factories.end())
        throw std::runtime_error("No scanner available for format '" + format + "'");
    return i->second();
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

    if (f == "yaml") return "yaml";
    if (f == "arkimet") return "arkimet";
    if (f == "metadata") return "arkimet";
    if (default_format) return default_format;
    throw std::runtime_error("unsupported format `" + format + "`");
}

std::string Scanner::format_from_filename(const std::string& fname, const char* default_format)
{
    // Extract the extension
    size_t epos = fname.rfind('.');
    if (epos != string::npos)
    {
        string ext = fname.substr(epos + 1);
        if (ext == "zip" || ext == "gz" || ext == "tar")
        {
            size_t epos1 = fname.rfind('.', epos - 1);
            ext = fname.substr(epos1 + 1, epos - epos1 - 1);
        }
        return normalise_format(str::lower(ext), default_format);
    }
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
