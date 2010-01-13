/*
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

#include <arki/types/test-utils.h>
#include <arki/types/bbox.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_types_bbox_shar {
};
TESTGRP(arki_types_bbox);

// Check INVALID
template<> template<>
void to::test<1>()
{
	Item<BBox> o = bbox::INVALID::create();
	ensure_equals(o->style(), BBox::INVALID);
	const bbox::INVALID* v = o->upcast<bbox::INVALID>();
	ensure(v);

	ensure_equals(o, Item<BBox>(bbox::INVALID::create()));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_BBOX);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<2>()
{
	Item<BBox> o = bbox::INVALID::create();

	tests::Lua test(
		"function test(o) \n"
		"  if o.style() ~= 'INVALID' then return 'style is '..o.style()..' instead of INVALID' end \n"
		"  if o.invalid() ~= nil then return 'o.invalid() gave'..o.invalid()..' instead of nil' end \n"
		"  if o.point() ~= nil then return 'o.point() gave '..o.point()..' instead of nil' end \n"
		"  if o.box() ~= nil then return 'o.box() gave '..o.box()..' instead of nil' end \n"
		"  if o.hull() ~= nil then return 'o.hull() gave '..o.hull()..' instead of nil' end \n"
		"  if tostring(o) ~= 'INVALID()' then return 'tostring gave '..tostring(o)..' instead of INVALID()' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

}

// vim:set ts=4 sw=4:
