/*
 * Copyright (C) 2007,2008  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/matcher/utils.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::matcher;

struct arki_matcher_utils_shar {
	arki_matcher_utils_shar()
	{
	}
};
TESTGRP(arki_matcher_utils);

// Check OptionalCommaList
template<> template<>
void to::test<1>()
{
	OptionalCommaList l("CIAO,,1,2,,3");
	ensure(l.has(0));
	ensure(!l.has(1));
	ensure(l.has(2));
	ensure(l.has(3));
	ensure(!l.has(4));
	ensure(l.has(5));
	ensure(!l.has(6));
	ensure(!l.has(100));

	ensure_equals(l.getInt(1, 100), 100);
	ensure_equals(l.getInt(2, 100), 1);
	ensure_equals(l.getInt(3, 100), 2);
	ensure_equals(l.getInt(4, 100), 100);
	ensure_equals(l.getInt(5, 100), 3);
	ensure_equals(l.getInt(6, 100), 100);
	ensure_equals(l.getInt(100, 100), 100);

	ensure_equals(l.getDouble(2), 1.0);
	ensure_equals(l.getDouble(3), 2.0);
	ensure_equals(l.getDouble(5), 3.0);
}

// Check CommaJoiner
template<> template<>
void to::test<2>()
{
	CommaJoiner j;
	j.add("ciao");
	j.addUndef();
	j.add(3);
	j.add(3.14);
	j.addUndef();
	j.addUndef();

	ensure_equals(j.join(), "ciao,,3,3.14");
}

}

// vim:set ts=4 sw=4:
