#include "inputs.h"
#include "config.h"
#include "io.h"
#include "arki/dataset.h"
#include "arki/utils/string.h"
#include "arki/nag.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

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

}
}
