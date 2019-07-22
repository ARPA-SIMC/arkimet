#include "config.h"
#include "run.h"
#include <sstream>
#include <iomanip>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchRun::name() const { return "run"; }

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
		minute = 
			strtoul(pattern.substr(0, pos).c_str(), 0, 10) * 60 +
			strtoul(pattern.substr(pos + 1).c_str(), 0, 10);
}

bool MatchRunMinute::matchItem(const Type& o) const
{
    const types::run::Minute* v = dynamic_cast<const types::run::Minute*>(&o);
    if (!v) return false;
    if (minute >= 0 && (unsigned)minute != v->minute()) return false;
    return true;
}

std::string MatchRunMinute::toString() const
{
	stringstream res;
	unsigned hour = minute / 60;
	unsigned min = minute % 60;
	res << "MINUTE," << setfill('0') << setw(2) << hour;
	if (min)
		res << ":" << setw(2) << min;
	return res.str();
}

unique_ptr<MatchRun> MatchRun::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else {
        name = str::strip(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+1);
    }
    switch (types::Run::parseStyle(name))
    {
        case types::Run::MINUTE: return unique_ptr<MatchRun>(new MatchRunMinute(rest));
        default: throw invalid_argument("cannot parse type of run to match: unsupported run style: " + name);
    }
}

void MatchRun::init()
{
    MatcherType::register_matcher("run", TYPE_RUN, (MatcherType::subexpr_parser)MatchRun::parse);
}

}
}
