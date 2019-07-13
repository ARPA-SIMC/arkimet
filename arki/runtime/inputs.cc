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

Inputs::Inputs(core::cfg::Sections& merged)
    : merged(merged)
{
}

Inputs::Inputs(core::cfg::Sections& merged, ScanCommandLine& args)
    : merged(merged)
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

        if (merged.empty())
            throw commandline::BadOption("you need to specify at least one input file or dataset");

        // Some things cannot be done when querying multiple datasets at the same time
        if (merged.size() > 1)
        {
            if (args.report->boolValue())
                throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
        }
    }
}

Inputs::Inputs(core::cfg::Sections& merged, QueryCommandLine& args)
    : merged(merged)
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

        if (merged.empty())
            throw commandline::BadOption("you need to specify at least one input file or dataset");

        // Filter the dataset list
        if (args.restr->isSet())
        {
            Restrict rest(args.restr->stringValue());
            remove_unallowed(rest);
            if (merged.empty())
                throw commandline::BadOption("no accessible datasets found for the given --restrict value");
        }

        // Some things cannot be done when querying multiple datasets at the same time
        if (merged.size() > 1 && !(args.qmacro->isSet()))
        {
            if (args.postprocess->boolValue())
                throw commandline::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
            if (args.report->boolValue())
                throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
        }
    }
}

void Inputs::add_section(const core::cfg::Section& section, const std::string& name_)
{
    std::string name = name_.empty() ? section.value("name") : name_;
    core::cfg::Section* old = merged.section(name);
    if (old)
    {
        nag::warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                section.value("name").c_str(), section.value("path").c_str(), old->value("path").c_str());
        return;
    }
    merged.emplace(name, section);
}

void Inputs::add_sections(const core::cfg::Sections& cfg)
{
    for (const auto& si: cfg)
        if (!si.second.empty())
            add_section(si.second, si.first);
}

void Inputs::add_config_file(const std::string& pathname)
{
    add_sections(parse_config_file(pathname));
}

void Inputs::add_pathname(const std::string& pathname)
{
    add_section(dataset::Reader::read_config(pathname));
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

    for (auto si: merged)
    {
        if (restrict.is_allowed(si.second))
            continue;
        to_remove.emplace_back(si.first);
    }

    for (const auto& name: to_remove)
        merged.erase(name);
}

void Inputs::remove_system_datasets()
{
    std::vector<std::string> to_remove;

    for (auto si: merged)
    {
        const std::string& type = si.second.value("type");
        const std::string& name = si.second.value("name");

        if (type == "error" || type == "duplicates" ||
            (type == "remote" && (name == "error" || name == "duplicates")))
            to_remove.emplace_back(si.first);
    }

    for (const auto& name: to_remove)
        merged.erase(name);
}

}
}
