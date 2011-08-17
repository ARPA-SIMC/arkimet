/*
 * dataset/index - Dataset index infrastructure
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/index/base.h>
#include <arki/metadata.h>
#include <arki/matcher.h>

#include <wibble/exception.h>
#include <wibble/regexp.h>
#include <wibble/string.h>

#include <sstream>
#include <ctime>

// FIXME: for debugging
//#include <iostream>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::dataset::index;

namespace arki {
namespace dataset {
namespace index {

std::set<types::Code> parseMetadataBitmask(const std::string& components)
{
	set<types::Code> res;
	Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
	for (Splitter::const_iterator i = splitter.begin(str::tolower(components));
			i != splitter.end(); ++i)
	{
		res.insert(types::parseCodeName(*i));
	}
	return res;
}

struct Adder
{
	string res;

	template<typename VAL>
	void add(const VAL& val, const char* name)
	{
		switch (val.size())
		{
			case 0: break;
			case 1: res += val.begin()->encode(); break;
			default:
					std::stringstream err;
					err << "can only handle metadata with 1 " << name << " (" << val.size() << " found)";
					throw wibble::exception::Consistency("generating ID", err.str());
		}
	}
};

std::string IDMaker::id(const Metadata& md) const
{
	string res;
	for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
		res += md.get(*i).encode();
	return str::encodeBase64(res);
}

std::string fmtin(const std::vector<int>& vals)
{
	if (vals.empty())
		throw NotFound();
	if (vals.size() == 1)
		return "="+str::fmt(*vals.begin());
	else
	{
		stringstream res;
		res << "IN(";
		for (vector<int>::const_iterator i = vals.begin();
				i != vals.end(); ++i)
			if (i == vals.begin())
				res << *i;
			else
				res << "," << *i;
		res << ")";
		return res.str();
	}
}

}
}
}
// vim:set ts=4 sw=4:
