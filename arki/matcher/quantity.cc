
/*
 * matcher/task - Task matcher
 *
 * Copyright (C) 2007,2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <arki/matcher/quantity.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>
#include <arki/types/utils.h>

#include <set>
#include <string>

using namespace std;
using namespace wibble;

namespace arki { namespace matcher {

/*============================================================================*/

std::string MatchQuantity::name() const { return "quantity"; }

MatchQuantity::MatchQuantity(const std::string& pattern)
{
	arki::types::split(pattern, values);
}

bool MatchQuantity::matchItem(const Item<>& o) const
{
	const types::Quantity* v = dynamic_cast<const types::Quantity*>(o.ptr());
	if (!v) return false;

	//se il matche specifica dei valori
	if (values.size())
	{
        //allora tutti i valori indicati devono essere presenti nell'oggetto
        for (std::set<std::string>::const_iterator i = values.begin();
                i != values.end(); ++i)
        {
            if (v->values.find(*i) == v->values.end())
                return false;
        }
	}

	return true;
}

std::string MatchQuantity::toString() const
{
    CommaJoiner res;
    res.add("QUANTITY");
    for (std::set<std::string>::const_iterator i = values.begin();
            i != values.end(); ++i)
        res.add(*i);
    return res.join();
}

MatchQuantity* MatchQuantity::parse(const std::string& pattern)
{
	return new MatchQuantity(pattern);
}

void MatchQuantity::init()
{
    Matcher::register_matcher("quantity", types::TYPE_QUANTITY, (MatcherType::subexpr_parser)MatchQuantity::parse);
}

}
}

// vim:set ts=4 sw=4:
