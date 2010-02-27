/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/querymacro.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/dataset/ondisk2.h>
#include <arki/scan/grib.h>
#include <arki/utils/lua.h>
#include <arki/utils/metadata.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;

struct arki_querymacro_shar {
	ConfigFile cfg;

	arki_querymacro_shar()
	{
		// Cleanup the test datasets
		system("rm -rf testds");
		system("mkdir testds");

		// In-memory dataset configuration
		string conf =
			"[testds]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"name = testds\n"
			"path = testds\n";
		stringstream incfg(conf);
		cfg.parse(incfg, "(memory)");

		// Import all data from test.grib1
		Metadata md;
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer testds(*cfg.section("testds"));
		size_t count = 0;
		while (scanner.next(md))
		{
			ensure(testds.acquire(md) == WritableDataset::ACQ_OK);
			++count;
		}
		ensure_equals(count, 3u);
		testds.flush();
	}
};
TESTGRP(arki_querymacro);

// Test running queries from Lua
template<> template<>
void to::test<1>()
{
	Querymacro qm(cfg, "test0", "foo");

	lua_getglobal(*qm.L, "count1");
	int count1 = lua_tointeger(*qm.L, -1);

	lua_getglobal(*qm.L, "count2");
	int count2 = lua_tointeger(*qm.L, -1);

	lua_pop(*qm.L, 2);

	ensure_equals(count1, 3);
	ensure_equals(count2, 1);
}

// Lua script that simply passes through the queries
template<> template<>
void to::test<2>()
{
	Querymacro qm(cfg, "noop", "testds");

	dataset::DataQuery dq;
	metadata::Collector mdc;
	qm.queryData(dq, mdc);
	ensure_equals(mdc.size(), 3u);
	ensure(mdc[0].source.defined());
	ensure(mdc[1].source.defined());
	ensure(mdc[2].source.defined());

	Summary s;
	qm.querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 3u);
}

// Lua script that simply passes through the queries, making temporary copies of data
template<> template<>
void to::test<3>()
{
	Querymacro qm(cfg, "noopcopy", "testds");

	dataset::DataQuery dq;
	metadata::Collector mdc;
	qm.queryData(dq, mdc);
	ensure_equals(mdc.size(), 3u);
	ensure(mdc[0].source.defined());
	ensure(mdc[1].source.defined());
	ensure(mdc[2].source.defined());

	Summary s;
	qm.querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 3u);
}

}

// vim:set ts=4 sw=4:
