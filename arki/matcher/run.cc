#include "run.h"
#include "arki/utils/string.h"
#include <iomanip>
#include <sstream>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchRun::name() const { return "run"; }

MatchRunMinute::MatchRunMinute(int minute) : minute(minute) {}

MatchRunMinute::MatchRunMinute(const std::string& pattern)
{
    if (pattern.empty())
    {
        minute = -1;
        return;
    }
    size_t pos = pattern.find(':');
    if (pos == string::npos)
        minute = strtoul(pattern.c_str(), 0, 10) * 60;
    else
        minute = strtoul(pattern.substr(0, pos).c_str(), 0, 10) * 60 +
                 strtoul(pattern.substr(pos + 1).c_str(), 0, 10);
}

MatchRunMinute* MatchRunMinute::clone() const
{
    return new MatchRunMinute(minute);
}

bool MatchRunMinute::matchItem(const Type& o) const
{
    const types::run::Minute* v = dynamic_cast<const types::run::Minute*>(&o);
    if (!v)
        return false;
    if (minute >= 0 && (unsigned)minute != v->get_Minute())
        return false;
    return true;
}

bool MatchRunMinute::match_buffer(types::Code code, const uint8_t* data,
                                  unsigned size) const
{
    if (code != TYPE_RUN)
        return false;
    if (size < 1)
        return false;
    if (types::Run::style(data, size) != run::Style::MINUTE)
        return false;
    if (minute >= 0 && (unsigned)minute != Run::get_Minute(data, size))
        return false;
    return true;
}

std::string MatchRunMinute::toString() const
{
    stringstream res;
    unsigned hour = minute / 60;
    unsigned min  = minute % 60;
    res << "MINUTE," << setfill('0') << setw(2) << hour;
    if (min)
        res << ":" << setw(2) << min;
    return res.str();
}

Implementation* MatchRun::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else
    {
        name = str::strip(pattern.substr(beg, pos - beg));
        rest = pattern.substr(pos + 1);
    }
    switch (types::Run::parseStyle(name))
    {
        case types::Run::Style::MINUTE: return new MatchRunMinute(rest);
        default:
            throw invalid_argument(
                "cannot parse type of run to match: unsupported run style: " +
                name);
    }
}

void MatchRun::init()
{
    MatcherType::register_matcher("run", TYPE_RUN,
                                  (MatcherType::subexpr_parser)MatchRun::parse);
}

} // namespace matcher
} // namespace arki
