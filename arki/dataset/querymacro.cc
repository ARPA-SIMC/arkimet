#include "querymacro.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

QueryMacro::QueryMacro(std::shared_ptr<Pool> pool, const std::string& name, const std::string& query)
    : dataset::Dataset(pool->session()), pool(pool), query(query)
{
    size_t pos = name.find(" ");
    if (pos == string::npos)
    {
        m_name = name;
    } else {
        m_name = name.substr(0, pos);
        macro_args = str::strip(name.substr(pos + 1));
    }
}

namespace {
std::vector<std::pair<std::string, std::function<std::shared_ptr<dataset::Reader>(const std::string& source, std::shared_ptr<QueryMacro> dataset)>>> parsers;
}

std::shared_ptr<Reader> QueryMacro::create_reader()
{
    for (const auto& entry: parsers)
    {
        std::string fname = arki::Config::get().dir_qmacro.find_file_noerror(m_name + "." + entry.first);
        if (!fname.empty())
        {
            return entry.second(fname, dynamic_pointer_cast<QueryMacro>(shared_from_this()));
        }
    }
    throw std::runtime_error("querymacro source not found for macro " + m_name);
}

namespace qmacro {

void register_parser(
        const std::string& ext,
        std::function<std::shared_ptr<dataset::Reader>(const std::string& source, std::shared_ptr<QueryMacro> opts)> parser)
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
