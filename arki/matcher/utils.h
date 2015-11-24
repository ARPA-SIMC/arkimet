#ifndef ARKI_MATCHER_UTILS
#define ARKI_MATCHER_UTILS

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

#include <arki/matcher.h>
#include <arki/utils/string.h>
#include <vector>
#include <string>
#include <stdint.h>

namespace arki {
namespace matcher {

struct OptionalCommaList : public std::vector<std::string>
{
	std::string tail;

	OptionalCommaList(const std::string& pattern, bool has_tail=false);

    bool has(size_t pos) const;
    int getInt(size_t pos, int def) const;
    unsigned getUnsigned(size_t pos, unsigned def) const;
    uint32_t getUnsignedWithMissing(size_t pos, uint32_t missing, bool& has_val) const;
    double getDouble(size_t pos, double def) const;
    const std::string& 	getString	(size_t pos, const std::string& def) const;

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
};

struct CommaJoiner : std::vector<std::string>
{
	size_t last;

	CommaJoiner() : last(0) {}

	template<typename T>
	void add(const T& val)
	{
		push_back(wibble::str::fmt(val));
		last = size();
	}

    template<typename T>
    void add(const T& val, const T& missing)
    {
        if (val == missing)
            push_back("-");
        else
            push_back(wibble::str::fmt(val));
        last = size();
    }

	void addUndef()
	{
		push_back(std::string());
	}

	std::string join() const
	{
		std::string res;
		for (size_t i = 0; i < last; ++i)
			if (res.empty())
				res += (*this)[i];
			else
				res += "," + (*this)[i];
		return res;
	}
};

}
}

// vim:set ts=4 sw=4:
#endif
