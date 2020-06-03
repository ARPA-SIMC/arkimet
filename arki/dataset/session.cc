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
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
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
{
    if (load_aliases)
        matcher_parser.load_system_aliases();
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

void Session::add_dataset(const core::cfg::Section& cfg)
{
    auto ds = dataset(cfg);
    if (dataset_pool.find(ds->name()) != dataset_pool.end())
        throw std::runtime_error(
                "cannot load dataset " + ds->name() + " multiple times");
    dataset_pool.emplace(ds->name(), ds);

    // TODO: handle merging remote aliases
}

bool Session::has_dataset(const std::string& name) const
{
    return dataset_pool.find(name) != dataset_pool.end();
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

std::shared_ptr<Dataset> Session::dataset(const std::string& name)
{
    auto res = dataset_pool.find(name);
    if (res == dataset_pool.end())
        throw std::runtime_error("dataset " + name + " not found in session pool");
    return res->second;
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

core::cfg::Sections Session::get_alias_database() const
{
    return matcher_parser.serialise_aliases();
}

std::string Session::expand_remote_query(const core::cfg::Sections& remotes, const std::string& query)
{
    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    std::set<std::string> servers_seen;
    std::string expanded;
    std::string resolved_by;
    bool first = true;
    for (auto si: remotes)
    {
        std::string server = si.second.value("server");
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

namespace {

struct FSPos
{
    dev_t dev;
    ino_t ino;

    FSPos(const struct stat& st)
        : dev(st.st_dev), ino(st.st_ino)
    {
    }

    bool operator<(const FSPos& o) const
    {
        if (dev < o.dev) return true;
        if (dev > o.dev) return false;
        return ino < o.ino;
    }

    bool operator==(const FSPos& o) const
    {
        return dev == o.dev && ino == o.ino;
    }
};

struct PathMatch
{
    std::set<FSPos> parents;

    PathMatch(const std::string& pathname)
    {
        fill_parents(pathname);
    }

    void fill_parents(const std::string& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        auto i = parents.insert(FSPos(st));
        // If we already knew of that fs position, stop here: we reached the
        // top or a loop
        if (i.second == false) return;
        // Otherwise, go up a level and scan again
        fill_parents(str::normpath(str::joinpath(pathname, "..")));
    }

    bool is_under(const std::string& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        return parents.find(FSPos(st)) != parents.end();
    }
};

}

std::shared_ptr<dataset::Dataset> Session::locate_metadata(Metadata& md)
{
    const auto& source = md.sourceBlob();
    std::string pathname = source.absolutePathname();

    PathMatch pmatch(pathname);

    for (const auto& dsi: dataset_pool)
    {
        auto lcfg = std::dynamic_pointer_cast<dataset::local::Dataset>(dsi.second);
        if (!lcfg) continue;
        if (pmatch.is_under(lcfg->path))
        {
            md.set_source(source.makeRelativeTo(lcfg->path));
            return dsi.second;
        }
    }

    return std::shared_ptr<dataset::local::Dataset>();
}

}
}