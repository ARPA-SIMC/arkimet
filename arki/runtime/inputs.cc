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
    if (args.stdin_input->isSet())
    {
        if (args.hasNext())
            throw commandline::BadOption("you cannot specify input files or datasets when using --stdin");
        if (args.files->isSet())
            throw commandline::BadOption("--stdin cannot be used together with --files");
    } else {
        if (args.files->isSet())    // From --files option, looking for data files or datasets
            add_pathnames_from_file(args.files->stringValue());

        while (args.hasNext()) // From command line arguments, looking for data files or datasets
            add_pathname(args.next());

        if (merged.section_begin() == merged.section_end())
            throw commandline::BadOption("you need to specify at least one input file or dataset");

        // Some things cannot be done when querying multiple datasets at the same time
        if (merged.sections_size() > 1)
        {
            if (args.report->boolValue())
                throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
        }
    }
}

Inputs::Inputs(QueryCommandLine& args)
{
    if (args.stdin_input->isSet())
    {
        if (args.hasNext())
            throw commandline::BadOption("you cannot specify input files or datasets when using --stdin");
        if (args.cfgfiles->isSet())
            throw commandline::BadOption("--stdin cannot be used together with --config");
        if (args.restr->isSet())
            throw commandline::BadOption("--stdin cannot be used together with --restr");
        if (args.qmacro->isSet())
            throw commandline::BadOption("--stdin cannot be used together with --qmacro");
    } else {
        // From -C options, looking for config files or datasets
        for (const auto& pathname : args.cfgfiles->values())
            add_config_file(pathname);

        while (args.hasNext()) // From command line arguments, looking for data files or datasets
            add_pathname(args.next());

        if (merged.section_begin() == merged.section_end())
            throw commandline::BadOption("you need to specify at least one input file or dataset");

        // Filter the dataset list
        if (args.restr->isSet())
        {
            Restrict rest(args.restr->stringValue());
            remove_unallowed(rest);
            if (merged.section_begin() == merged.section_end())
                throw commandline::BadOption("no accessible datasets found for the given --restrict value");
        }

        // Some things cannot be done when querying multiple datasets at the same time
        if (merged.sections_size() > 1 && !(args.qmacro->isSet()))
        {
            if (args.postprocess->boolValue())
                throw commandline::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
            if (args.report->boolValue())
                throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
        }
    }
}

void Inputs::add_section(const ConfigFile& section)
{
    ConfigFile* old = merged.section(section.value("name"));
    if (old)
    {
        nag::warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                section.value("name").c_str(), section.value("path").c_str(), old->value("path").c_str());
        return;
    }
    merged.mergeInto(section.value("name"), section);
}

void Inputs::add_sections(const ConfigFile& cfg)
{
    for (auto i = cfg.section_begin(); i != cfg.section_end(); ++i)
        if (i->second->valueSize())
            add_section(*i->second);
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
    dataset::Reader::read_config(pathname, cfg);
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
    std::vector<std::string> to_remove;

    for (auto si = merged.section_begin(); si != merged.section_end(); ++si)
    {
        if (restrict.is_allowed(*si->second))
            continue;
        to_remove.emplace_back(si->first);
    }

    for (const auto& name: to_remove)
        merged.delete_section(name);
}

void Inputs::remove_system_datasets()
{
    std::vector<std::string> to_remove;

    for (auto si = merged.section_begin(); si != merged.section_end(); ++si)
    {
        const std::string& type = si->second->value("type");
        const std::string& name = si->second->value("name");

        if (type == "error" || type == "duplicates" ||
            (type == "remote" && (name == "error" || name == "duplicates")))
            to_remove.emplace_back(si->first);
    }

    for (const auto& name: to_remove)
        merged.delete_section(name);
}

ConfigFile Inputs::as_config() const
{
    return merged;
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
    for (auto si = merged.section_begin(); si != merged.section_end(); ++si)
    {
        string server = si->second->value("server");
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
