/*
 * matcher/run - Run matcher
 *
 * Copyright (C) 2008--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/matcher/run.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>
#include <sstream>
#include <iomanip>

using namespace std;
using namespace wibble;
using namespace arki::types;

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

MatchRun* MatchRun::parse(const std::string& pattern)
{
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
}

void MatchRun::init()
{
    Matcher::register_matcher("run", types::TYPE_RUN, (MatcherType::subexpr_parser)MatchRun::parse);
}

}
}

// vim:set ts=4 sw=4:
