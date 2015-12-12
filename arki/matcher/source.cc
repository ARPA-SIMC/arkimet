#include "config.h"

#include <arki/matcher/source.h>
#include <arki/matcher/utils.h>
#include <sstream>
#include <iomanip>

using namespace std;

namespace arki {
namespace matcher {

std::string MatchSource::name() const { return "source"; }

#if 0
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

bool MatchRunMinute::matchItem(const Item<>& o) const
{
	const types::run::Minute* v = dynamic_cast<const types::run::Minute*>(o.ptr());
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
#endif

unique_ptr<MatchSource> MatchSource::parse(const std::string& pattern)
{
#if 0
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::trim(pattern.substr(beg));
    else {
        name = str::trim(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+1);
    }
    switch (types::Run::parseStyle(name))
    {
        case types::Run::MINUTE: return new MatchRunMinute(rest);
        default:
                                 throw wibble::exception::Consistency("parsing type of run to match", "unsupported run style: " + name);
    }
#endif
}

void MatchSource::init()
{
    Matcher::register_matcher("source", types::TYPE_SOURCE, (MatcherType::subexpr_parser)MatchSource::parse);
}

}
}

// vim:set ts=4 sw=4:
