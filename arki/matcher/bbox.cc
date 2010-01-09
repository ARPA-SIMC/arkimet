/*
 * matcher/bbox - Bounding box matcher
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

#include <arki/matcher/bbox.h>
#include <arki/matcher/utils.h>
#include <arki/types/bbox.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchBBox::name() const { return "bbox"; }

MatchBBoxExact::MatchBBoxExact(const std::string& pattern)
	: sample(types::BBox::decodeString(pattern))
{
}

bool MatchBBoxExact::matchItem(const Item<>& o) const
{
	return sample->compare(*o) == 0;
}

std::string MatchBBoxExact::toString() const
{
	stringstream out;
	out << "is " << sample;
	return out.str();
}

MatchBBox* MatchBBox::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(' ', beg);
	string verb;
	string rest;
	if (pos == string::npos)
		verb = str::tolower(str::trim(pattern.substr(beg)));
	else {
		verb = str::tolower(str::trim(pattern.substr(beg, pos-beg)));
		rest = str::trim(pattern.substr(pos+1));
	}
	if (verb == "is")
	{
		return new MatchBBoxExact(rest);
	} else {
		throw wibble::exception::Consistency("parsing type of bbox match", "unsupported match type: " + verb);
	}
}

MatcherType bbox("bbox", types::TYPE_BBOX, (MatcherType::subexpr_parser)MatchBBox::parse);

}
}

// vim:set ts=4 sw=4:
