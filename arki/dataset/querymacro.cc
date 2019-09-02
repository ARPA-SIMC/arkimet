#include "querymacro.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace qmacro {

Options::Options(const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query)
    : datasets_cfg(datasets_cfg), query(query)
{
    size_t pos = name.find(" ");
    if (pos == string::npos)
    {
        macro_name = name;
    } else {
        macro_name = name.substr(0, pos);
        macro_args = str::strip(name.substr(pos + 1));
    }

}

Options::Options(const core::cfg::Section& macro_cfg, const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query)
    : macro_cfg(macro_cfg), datasets_cfg(datasets_cfg), query(query)
{
    size_t pos = name.find(" ");
    if (pos == string::npos)
    {
        macro_name = name;
    } else {
        macro_name = name.substr(0, pos);
        macro_args = str::strip(name.substr(pos + 1));
    }

}


Base::Base(const Options& opts)
    : m_config(std::shared_ptr<const dataset::Config>(new dataset::Config(opts.macro_cfg))),
      datasets_cfg(opts.datasets_cfg)
{
}

Base::~Base()
{
}


std::vector<std::pair<std::string, std::function<std::shared_ptr<dataset::Reader>(const std::string& source, const Options& opts)>>> parsers;

std::shared_ptr<dataset::Reader> get(const Options& opts)
{
    for (const auto& entry: parsers)
    {
        std::string fname = arki::Config::get().dir_qmacro.find_file_noerror(opts.macro_name + "." + entry.first);
        if (!fname.empty())
        {
            return entry.second(fname, opts);
        }
    }
    throw std::runtime_error("querymacro source not found for macro " + opts.macro_name);
}

void register_parser(
        const std::string& ext,
        std::function<std::shared_ptr<dataset::Reader>(const std::string& source, const Options& opts)> parser)
{
    for (const auto& entry: parsers)
        if (entry.first == ext)
            throw std::runtime_error("querymacro parser for ." + ext + " files has already been registered");

    parsers.emplace_back(make_pair(ext, parser));
}

void init()
{
}

}
}
}
