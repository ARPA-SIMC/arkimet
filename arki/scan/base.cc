#include "base.h"
#include "arki/libconfig.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/core/file.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#ifdef HAVE_GRIBAPI
#include "arki/scan/grib.h"
#endif
#ifdef HAVE_DBALLE
#include "arki/scan/bufr.h"
#endif
#ifdef HAVE_HDF5
#include "arki/scan/odimh5.h"
#endif
#ifdef HAVE_VM2
#include "arki/scan/vm2.h"
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

Scanner::~Scanner()
{
}

void Scanner::open(const std::string& filename, const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relpath = relpath;
    reader = segment::Reader::for_pathname(utils::get_format(filename), basedir, relpath, filename, lock);
}

void Scanner::close()
{
    filename.clear();
    basedir.clear();
    relpath.clear();
    reader.reset();
}

void Scanner::test_open(const std::string& filename)
{
    string basedir, relpath;
    utils::files::resolve_path(filename, basedir, relpath);
    open(sys::abspath(filename), basedir, relpath, make_shared<core::lock::Null>());
}

bool Scanner::scan_file(const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    open(abspath, root, relpath, lock);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

std::unique_ptr<Scanner> Scanner::get_scanner(const std::string& format)
{
#ifdef HAVE_GRIBAPI
    if (format == "grib" || format == "grib1" || format == "grib2")
        return std::unique_ptr<Scanner>(new scan::Grib);
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return std::unique_ptr<Scanner>(new scan::Bufr);
#endif
#ifdef HAVE_HDF5
    if ((format == "h5") || (format == "odim") || (format == "odimh5"))
        return std::unique_ptr<Scanner>(new scan::OdimH5);
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return std::unique_ptr<Scanner>(new scan::Vm2);
#endif
    throw std::runtime_error("No scanner available for format '" + format + "'");
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
#ifdef HAVE_HDF5
    if (format == "odimh5")
        return odimh5::validator();
#endif
#ifdef HAVE_VM2
   if (format == "vm2")
       return vm2::validator();
#endif
    throw std::runtime_error("No validator available for format '" + format + "'");
}

}
}
