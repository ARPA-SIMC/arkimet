/*
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
	m.writeYaml(o);
	return o;
}
}

namespace tut {
using namespace std;
using namespace arki;

struct arki_metadata_consumer_shar {
	arki_metadata_consumer_shar()
	{
	}
};
TESTGRP(arki_metadata_consumer);

// Test boilerplate
template<> template<>
void to::test<1>()
{
#ifdef HAVE_LUA

#if 0
	md.source = source::Blob::create("grib", "fname", 1, 2);
	fill(md);

	tests::Lua test(
		"function test(md) \n"
		"  if md.source == nil then return 'source is nil' end \n"
		"  if md.origin == nil then return 'origin is nil' end \n"
		"  if md.product == nil then return 'product is nil' end \n"
		"  if md.level == nil then return 'level is nil' end \n"
		"  if md.timerange == nil then return 'timerange is nil' end \n"
		"  if md.area == nil then return 'area is nil' end \n"
		"  if md.ensemble == nil then return 'ensemble is nil' end \n"
		"  if md.assigneddataset == nil then return 'assigneddataset is nil' end \n"
		"  if md.reftime == nil then return 'reftime is nil' end \n"
		"  if md.bbox ~= nil then return 'bbox is not nil' end \n"
		"  notes = md.notes \n"
		"  if table.getn(notes) ~= 1 then return 'table has '..table.getn(notes)..' elements instead of 1' end \n"
		"  i = 0 \n"
		"  for name, value in md.iter do \n"
		"    i = i + 1 \n"
		"  end \n"
		"  if i ~= 8 then return 'iterated '..i..' items instead of 8' end \n"
		"  return nil\n"
		"end \n"
	);
	test.pusharg(md);
	ensure_equals(test.run(), "");
#endif

#endif
}

}

// vim:set ts=4 sw=4:
