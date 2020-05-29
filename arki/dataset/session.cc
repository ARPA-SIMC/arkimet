#include "session.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/segment.h"
#include "arki/segment/fd.h"
#include "arki/segment/dir.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/dataset/file.h"
#include "arki/dataset/ondisk2.h"
#include "arki/dataset/iseg.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/empty.h"
#include "arki/dataset/fromfunction.h"
#include "arki/dataset/testlarge.h"
#include "arki/libconfig.h"

#ifdef HAVE_LIBCURL
#include "arki/dataset/http.h"
#endif

using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

Session::Session()
{
}

Session::~Session()
{
}

std::shared_ptr<segment::Reader> Session::segment_reader(const std::string& format, const std::string& root, const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    std::string abspath = str::joinpath(root, relpath);
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = Segment::detect_reader(format, root, relpath, str::joinpath(root, relpath), lock);
        reader_pool[abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::Writer> Session::segment_writer(const std::string& format, const std::string& root, const std::string& relpath)
{
    // Ensure that the directory containing the segment exists
    std::string abspath = str::joinpath(root, relpath);
    sys::makedirs(str::dirname(abspath));

    auto res(Segment::detect_writer(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        res.reset(new segment::dir::Writer(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Writer(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "cannot writer for " + format + " file " + relpath +
                ": format not supported");
    }
    return res;
}

std::shared_ptr<segment::Checker> Session::segment_checker(const std::string& format, const std::string& root, const std::string& relpath)
{
    std::string abspath = str::joinpath(root, relpath);

    auto res(Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        res.reset(new segment::dir::Checker(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Checker(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "getting writer for " + format + " file " + relpath +
                ": format not supported");
    }
    return res;
}

std::shared_ptr<Dataset> Session::dataset(const core::cfg::Section& cfg)
{
    std::string type = str::lower(cfg.value("type"));

    if (type == "iseg")
        return std::make_shared<iseg::Dataset>(shared_from_this(), cfg);
    if (type == "ondisk2")
        return std::make_shared<ondisk2::Dataset>(shared_from_this(), cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return std::make_shared<simple::Dataset>(shared_from_this(), cfg);
#ifdef HAVE_LIBCURL
    if (type == "remote")
        return std::make_shared<http::Dataset>(shared_from_this(), cfg);
#endif
    if (type == "outbound")
        return std::make_shared<outbound::Dataset>(shared_from_this(), cfg);
    if (type == "discard")
        return std::make_shared<empty::Dataset>(shared_from_this(), cfg);
    if (type == "file")
        return file::Dataset::from_config(shared_from_this(), cfg);
    if (type == "fromfunction")
        return std::make_shared<fromfunction::Dataset>(shared_from_this(), cfg);
    if (type == "testlarge")
        return std::make_shared<testlarge::Dataset>(shared_from_this(), cfg);

    throw std::runtime_error("cannot use configuration: unknown dataset type \""+type+"\"");
}

core::cfg::Section Session::read_config(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Section::parse(in);
    }

    // Remove trailing slashes, if any
    std::string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    std::unique_ptr<struct stat> st = sys::stat(fname);

    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.find(':');
        if (pos == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot read configuration from " << fname << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

        std::string prefix = path.substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_section(path);
        else
#endif
            return dataset::file::read_config(path);
    }

    if (S_ISDIR(st->st_mode))
        return dataset::local::Reader::read_config(fname);
    else
        return dataset::file::read_config(fname);
}

core::cfg::Sections Session::read_configs(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Sections::parse(in);
    }

    // Remove trailing slashes, if any
    std::string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    std::unique_ptr<struct stat> st = sys::stat(fname);

    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.find(':');
        if (pos == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot read configuration from " << fname << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

        std::string prefix = path.substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_sections(path);
        else
#endif
            return dataset::file::read_configs(path);
    }

    if (S_ISDIR(st->st_mode))
    {
        // A directory, read the dataset config
        return dataset::local::Reader::read_configs(fname);
    }
    else
    {
        // A file, check for known extensions
        std::string format = scan::Scanner::format_from_filename(fname, "");
        if (!format.empty())
            return dataset::file::read_configs(fname);

        // Read the contents as configuration
        sys::File in(fname, O_RDONLY);
        return core::cfg::Sections::parse(in);
    }
}

Matcher Session::matcher(const std::string& expr)
{
    return matcher_parser.parse(expr);
}

}
}
