/*
 * matcher/utils - Support code to implement matchers
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/utils.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

OptionalCommaList::OptionalCommaList(const std::string& pattern, bool has_tail)
{
	string p;
	if (has_tail)
	{
		size_t pos = pattern.find(":");
		if (pos == string::npos)
			p = pattern;
		else
		{
			p = str::trim(pattern.substr(0, pos));
			tail = str::trim(pattern.substr(pos+1));
		}
	} else
		p = pattern;
	str::Split split(",", p);
	for (str::Split::const_iterator i = split.begin(); i != split.end(); ++i)
		push_back(*i);
	while (!empty() && (*this)[size() - 1].empty())
		resize(size() - 1);
}

bool OptionalCommaList::has(size_t pos) const
{
	if (pos >= size()) return false;
	if ((*this)[pos].empty()) return false;
	return true;
}

int OptionalCommaList::getInt(size_t pos, int def) const
{
	if (!has(pos)) return def;
	return strtoul((*this)[pos].c_str(), 0, 10);
}

double OptionalCommaList::getDouble(size_t pos) const
{
	return strtod((*this)[pos].c_str(), 0);
}

#if 0
	bool matchInt(size_t pos, int val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtoul((*this)[pos].c_str(), 0, 10);
		return req == val;
	}

	bool matchDouble(size_t pos, double val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtod((*this)[pos].c_str(), 0);
		return req == val;
	}

	std::string toString() const
	{
		string res;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (res.empty())
				res += *i;
			else
				res += "," + *i;
		return res;
	}

	template<typename V>
	bool matchVals(const V* vals, size_t size) const
	{
		for (size_t i = 0; i < size; ++i)
			if (this->size() > i && !(*this)[i].empty() && (V)strtoul((*this)[i].c_str(), 0, 10) != vals[i])
				return false;
		return true;
	}

	std::string make_sql(const std::string& column, const std::string& style, size_t maxvals) const
	{
		string res = column + " LIKE '" + style;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
		{
			res += ',';
			if (i->empty())
				res += '%';
			else
				res += *i;
		}
		if (size() < maxvals)
			res += ",%";
		return res + '\'';
	}

	std::vector< std::pair<int, bool> > getIntVector() const
	{
		vector< pair<int, bool> > res;

		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (i->empty())
				res.push_back(make_pair(0, false));
			else
				res.push_back(make_pair(strtoul(i->c_str(), 0, 10), true));

		return res;
	}
#endif

}
}

// vim:set ts=4 sw=4:
