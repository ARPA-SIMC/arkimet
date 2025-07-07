#include "session.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/dataset/empty.h"
#include "arki/dataset/file.h"
#include "arki/dataset/fromfunction.h"
#include "arki/dataset/iseg.h"
#include "arki/dataset/ondisk2.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/simple.h"
#include "arki/dataset/testlarge.h"
#include "arki/libconfig.h"
#include "arki/nag.h"
#include "arki/scan.h"
#include "arki/segment/data.h"
#include "arki/segment/data/dir.h"
#include "arki/segment/data/fd.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
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

Session::~Session() {}

std::shared_ptr<Dataset> Session::dataset(const core::cfg::Section& cfg)
{
    std::string type = str::lower(cfg.value("type"));

    if (type == "iseg")
        return std::make_shared<iseg::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "ondisk2")
        return std::make_shared<ondisk2::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return std::make_shared<simple::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
#ifdef HAVE_LIBCURL
    if (type == "remote")
        return std::make_shared<http::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
#endif
    if (type == "outbound")
        return std::make_shared<outbound::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "discard")
        return std::make_shared<empty::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "file")
        return file::Dataset::from_config(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "fromfunction")
        return std::make_shared<fromfunction::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);
    if (type == "testlarge")
        return std::make_shared<testlarge::Dataset>(
            std::dynamic_pointer_cast<Session>(shared_from_this()), cfg);

    throw std::runtime_error("cannot use configuration for \"" +
                             cfg.value("name") + "\": unknown dataset type \"" +
                             type + "\"");
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

std::shared_ptr<core::cfg::Section>
Session::read_config(const std::filesystem::path& path)
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
            ss << "cannot read configuration from " << path
               << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

        std::string prefix = path.native().substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_section(path);
        else
#endif
            return dataset::file::read_config(prefix,
                                              path.native().substr(pos + 1));
    }

    if (S_ISDIR(st->st_mode))
        return dataset::local::Reader::read_config(path);
    else if (path.filename() == "config")
        return dataset::local::Reader::read_config(path.parent_path());
    else if (auto res = dataset::file::read_config(path))
        return res;
    else
        throw std::runtime_error("unsupported input file " + path.native());
}

std::shared_ptr<core::cfg::Sections>
Session::read_configs(const std::filesystem::path& path)
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
            ss << "cannot read configuration from " << path
               << " because it does not exist";
            throw std::runtime_error(ss.str());
        }

        std::string prefix = path.native().substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_sections(path);
        else
#endif
            return dataset::file::read_configs(prefix,
                                               path.native().substr(pos + 1));
    }

    if (S_ISDIR(st->st_mode))
    {
        // A directory, read the dataset config
        return dataset::local::Reader::read_configs(path);
    }
    else
    {
        // A file, check for known extensions
        if (auto res = dataset::file::read_configs(path))
            return res;

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

std::string
Session::expand_remote_query(std::shared_ptr<const core::cfg::Sections> remotes,
                             const std::string& query)
{
    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    std::set<std::string> servers_seen;
    std::string expanded;
    std::string resolved_by;
    bool first = true;
    for (auto si : *remotes)
    {
        std::string server = si.second->value("server");
        if (servers_seen.find(server) != servers_seen.end())
            continue;
        std::string got;
        try
        {
            if (server.empty())
            {
                got         = matcher_parser.parse(query).toStringExpanded();
                resolved_by = "local system";
            }
            else
            {
                got = dataset::http::Reader::expandMatcher(query, server);
                resolved_by = server;
            }
        }
        catch (std::exception& e)
        {
            // If the server cannot expand the query, we're
            // ok as we send it expanded. What we are
            // checking here is that the server does not
            // have a different idea of the same aliases
            // that we use
            continue;
        }
        if (!first && got != expanded)
        {
            nag::warning("%s expands the query as %s", server.c_str(),
                         got.c_str());
            nag::warning("%s expands the query as %s", resolved_by.c_str(),
                         expanded.c_str());
            throw std::runtime_error(
                "cannot check alias consistency: two systems queried disagree "
                "about the query alias expansion");
        }
        else if (first)
            expanded = got;
        first = false;
    }

    if (!first)
        return expanded;
    return query;
}

} // namespace dataset
} // namespace arki
