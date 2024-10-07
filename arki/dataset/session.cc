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
#include "arki/dataset/simple.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/empty.h"
#include "arki/dataset/fromfunction.h"
#include "arki/dataset/testlarge.h"
#include "arki/nag.h"
#include "arki/libconfig.h"
#include <memory>

#ifdef HAVE_LIBCURL
#include "arki/dataset/http.h"
#endif

using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

Session::Session(bool load_aliases)
    : reader_pool()
{
    if (load_aliases)
        matcher_parser.load_system_aliases();
}

Session::~Session()
{
}

std::shared_ptr<segment::Reader> Session::segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto abspath = std::filesystem::weakly_canonical(root / relpath);
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = Segment::detect_reader(format, root, relpath, abspath, lock);
        reader_pool[abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::Writer> Session::segment_writer(const segment::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto abspath = root / relpath;
    std::filesystem::create_directories(abspath.parent_path());

    auto res(Segment::detect_writer(config, format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Writer(config, format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Writer(config, format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim" || format == "nc" || format == "jpeg") {
        res.reset(new segment::dir::Writer(config, format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Writer(config, format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "cannot create writer for " + format + " file " + relpath.native() +
                ": format not supported");
    }
    return res;
}

std::shared_ptr<segment::Checker> Session::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    std::string abspath = root / relpath;

    auto res(Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim" || format == "nc" || format == "jpeg") {
        res.reset(new segment::dir::Checker(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Checker(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "getting writer for " + format + " file " + relpath.native() +
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

    throw std::runtime_error("cannot use configuration for \"" + cfg.value("name") + "\": unknown dataset type \""+type+"\"");
}

#if 0
static std::string geturlprefix(const std::string& s)
{
    // Take until /dataset/
    size_t pos = s.rfind("/dataset/");
    if (pos == std::string::npos) return std::string();
    return s.substr(0, pos);
}
#endif

std::shared_ptr<core::cfg::Section> Session::read_config(const std::filesystem::path& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Section::parse(in);
        // TODO: normalize path
    }

    std::unique_ptr<struct stat> st = sys::stat(path);
    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.native().find(':');
        if (pos == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot read configuration from " << path << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

#ifdef HAVE_LIBCURL
        std::string prefix = path.native().substr(0, pos);
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_section(path);
        else
#endif
            return dataset::file::read_config(path);
    }

    if (S_ISDIR(st->st_mode))
        return dataset::local::Reader::read_config(path);
    else if (path.filename() == "config")
        return dataset::local::Reader::read_config(path.parent_path());
    else
        return dataset::file::read_config(path);
}

std::shared_ptr<core::cfg::Sections> Session::read_configs(const std::filesystem::path& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Sections::parse(in);
    }

    std::unique_ptr<struct stat> st = sys::stat(path);
    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.native().find(':');
        if (pos == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot read configuration from " << path << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

#ifdef HAVE_LIBCURL
        std::string prefix = path.native().substr(0, pos);
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_sections(path);
        else
#endif
            return dataset::file::read_configs(path);
    }

    if (S_ISDIR(st->st_mode))
    {
        // A directory, read the dataset config
        return dataset::local::Reader::read_configs(path);
    }
    else
    {
        // A file, check for known extensions
        std::string format = scan::Scanner::format_from_filename(path, "");
        if (!format.empty())
            return dataset::file::read_configs(path);

        // Read the contents as configuration
        sys::File in(path, O_RDONLY);
        return core::cfg::Sections::parse(in);
    }
}

Matcher Session::matcher(const std::string& expr)
{
    return matcher_parser.parse(expr);
}

std::shared_ptr<core::cfg::Sections> Session::get_alias_database() const
{
    return matcher_parser.serialise_aliases();
}

void Session::load_aliases(const core::cfg::Sections& aliases)
{
    matcher_parser.load_aliases(aliases);
}

void Session::load_remote_aliases(const std::string& url)
{
    matcher_parser.load_remote_aliases(url);
}

std::string Session::expand_remote_query(std::shared_ptr<const core::cfg::Sections> remotes, const std::string& query)
{
    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    std::set<std::string> servers_seen;
    std::string expanded;
    std::string resolved_by;
    bool first = true;
    for (auto si: *remotes)
    {
        std::string server = si.second->value("server");
        if (servers_seen.find(server) != servers_seen.end()) continue;
        std::string got;
        try {
            if (server.empty())
            {
                got = matcher_parser.parse(query).toStringExpanded();
                resolved_by = "local system";
            } else {
                got = dataset::http::Reader::expandMatcher(query, server);
                resolved_by = server;
            }
        } catch (std::exception& e) {
            // If the server cannot expand the query, we're
            // ok as we send it expanded. What we are
            // checking here is that the server does not
            // have a different idea of the same aliases
            // that we use
            continue;
        }
        if (!first && got != expanded)
        {
            nag::warning("%s expands the query as %s", server.c_str(), got.c_str());
            nag::warning("%s expands the query as %s", resolved_by.c_str(), expanded.c_str());
            throw std::runtime_error("cannot check alias consistency: two systems queried disagree about the query alias expansion");
        } else if (first)
            expanded = got;
        first = false;
    }

    if (!first)
        return expanded;
    return query;
}

std::shared_ptr<segment::Reader> DirSegmentsSession::segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto abspath = std::filesystem::weakly_canonical(root / relpath);
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = Segment::detect_reader(format, root, relpath, root / relpath, lock);
        reader_pool[abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::Writer> DirSegmentsSession::segment_writer(const segment::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto abspath = root / relpath;
    std::filesystem::create_directories(abspath.parent_path());

    auto res(Segment::detect_writer(config, format, root, relpath, abspath, false));
    if (res) return res;

    res.reset(new segment::dir::Writer(config, format, root, relpath, abspath));
    return res;
}

std::shared_ptr<segment::Checker> DirSegmentsSession::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto abspath = root / relpath;

    auto res(Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    res.reset(new segment::dir::Checker(format, root, relpath, abspath));
    return res;
}

}
}
