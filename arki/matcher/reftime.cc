/*
 * matcher/reftime - Reftime matcher
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/reftime.h>
#include <arki/matcher/reftime/parser.h>
#include <arki/matcher/utils.h>
#include <arki/types/time.h>
#include <cctype>

using namespace std;
using namespace wibble;
using namespace arki::matcher::reftime;

namespace arki {
namespace matcher {

MatchReftime::MatchReftime(const std::string& pattern)
{
	// TODO: error reporting needs work
	Parser p;
	p.parse(pattern);

	// Copy the results into tests
	tests = p.res;
	p.res.clear();
}

MatchReftime::~MatchReftime()
{
	for (vector<DTMatch*>::iterator i = tests.begin(); i != tests.end(); ++i)
		delete *i;
}

std::string MatchReftime::name() const { return "reftime"; }

bool MatchReftime::matchItem(const Item<>& o) const
{
	if (const types::reftime::Position* po = dynamic_cast<const types::reftime::Position*>(o.ptr()))
	{
		for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
			if (!(*i)->match(po->time->vals))
				return false;
		return true;
	}
	else if (const types::reftime::Period* pe = dynamic_cast<const types::reftime::Period*>(o.ptr()))
	{
		for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
			if (!(*i)->match(pe->begin->vals, pe->end->vals))
				return false;
		return true;
	}
	return true;
}

std::string MatchReftime::sql(const std::string& column) const
{
	bool first = true;
	string res = "(";
	for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
	{
		if (first)
			first = false;
		else
			res += " AND ";
		res += (*i)->sql(column);
	}
	return res + ")";
}

void MatchReftime::dateRange(UItem<types::Time>& begin, UItem<types::Time>& end) const
{
	begin.clear();
	end.clear();
	for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
		(*i)->restrictDateRange(begin, end);
}

std::string MatchReftime::toString() const
{
	string res;
	for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
	{
		if (!res.empty() && (*i)->isLead())
			res += ",";
		res += (*i)->toString();
	}
	return res;
}

MatchReftime* MatchReftime::parse(const std::string& pattern)
{
	return new MatchReftime(pattern);
}


void MatchReftime::init()
{
    Matcher::register_matcher("reftime", types::TYPE_REFTIME, (MatcherType::subexpr_parser)MatchReftime::parse);
}

}
}

// vim:set ts=4 sw=4:
