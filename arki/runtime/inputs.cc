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
