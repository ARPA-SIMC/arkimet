#include "inputs.h"
#include "config.h"
#include "io.h"
#include "arki/runtime.h"
#include "arki/dataset.h"
#include "arki/dataset/http.h"
#include "arki/core/file.h"
#include "arki/utils/string.h"
#include "arki/nag.h"

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace runtime {

Inputs::Inputs(ScanCommandLine& args)
{
    if (args.files->isSet())    // From --files option, looking for data files or datasets
        add_pathnames_from_file(args.files->stringValue());

    while (args.hasNext()) // From command line arguments, looking for data files or datasets
        add_pathname(args.next());

    if (empty())
        throw commandline::BadOption("you need to specify at least one input file or dataset");

    // Some things cannot be done when querying multiple datasets at the same time
    if (size() > 1)
    {
        if (args.report->boolValue())
            throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
    }
}

Inputs::Inputs(QueryCommandLine& args)
{
    // From -C options, looking for config files or datasets
    for (const auto& pathname : args.cfgfiles->values())
        add_config_file(pathname);

    while (args.hasNext()) // From command line arguments, looking for data files or datasets
        add_pathname(args.next());

    if (empty())
        throw commandline::BadOption("you need to specify at least one input file or dataset");

    // Filter the dataset list
    if (args.restr->isSet())
    {
        Restrict rest(args.restr->stringValue());
        remove_unallowed(rest);
        if (empty())
            throw commandline::BadOption("no accessible datasets found for the given --restrict value");
    }

    // Some things cannot be done when querying multiple datasets at the same time
    if (size() > 1 && !(args.qmacro->isSet()))
    {
        if (args.postprocess->boolValue())
            throw commandline::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
        if (args.report->boolValue())
            throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
    }
}

void Inputs::add_sections(const ConfigFile& cfg)
{
    for (auto i = cfg.sectionBegin(); i != cfg.sectionEnd(); ++i)
        if (i->second->valueSize())
            emplace_back(*i->second);
}

void Inputs::add_config_file(const std::string& pathname)
{
    ConfigFile cfg;
    parseConfigFile(cfg, pathname);
    add_sections(cfg);
}

void Inputs::add_pathname(const std::string& pathname)
{
    ConfigFile cfg;
    dataset::Reader::readConfig(pathname, cfg);
    add_sections(cfg);
}

void Inputs::add_pathnames_from_file(const std::string& pathname)
{
    // Open the file
    unique_ptr<NamedFileDescriptor> in;
    if (pathname == "-")
        in.reset(new Stdin);
    else
        in.reset(new InputFile(pathname));

    // Read the content and scan the related files or dataset directories
    auto reader = LineReader::from_fd(*in);
    string line;
    while (reader->getline(line))
    {
        line = str::strip(line);
        if (line.empty())
            continue;
        add_pathname(line);
    }
}

void Inputs::remove_unallowed(const Restrict& restrict)
{
    for (unsigned idx = 0; idx < size(); )
    {
        ConfigFile& cfg = (*this)[idx];

        if (restrict.is_allowed(cfg))
            ++idx;
        else
            erase(begin() + idx);
    }
}

ConfigFile Inputs::as_config() const
{
    ConfigFile res;
    for (const auto& cfg: *this)
    {
        ConfigFile* old = res.section(cfg.value("name"));
        if (old)
        {
            nag::warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                    cfg.value("name").c_str(), cfg.value("path").c_str(), old->value("path").c_str());
            continue;
        }
        res.mergeInto(cfg.value("name"), cfg);
    }
    return res;
}

std::string Inputs::expand_remote_query(const std::string& query)
{
    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    set<string> servers_seen;
    string expanded;
    string resolved_by;
    bool first = true;
    for (const auto& i: *this)
    {
        string server = i.value("server");
        if (servers_seen.find(server) != servers_seen.end()) continue;
        string got;
        try {
            if (server.empty())
            {
                got = Matcher::parse(query).toStringExpanded();
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

}
}
