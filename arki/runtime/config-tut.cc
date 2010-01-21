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
#include <arki/runtime/config.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::runtime;

struct arki_runtime_config_shar {
};
TESTGRP(arki_runtime_config);

// Test restrict functions
template<> template<>
void to::test<1>()
{
	ConfigFile empty_cfg;
	ConfigFile allowed_cfg;
	ConfigFile unallowed_cfg;
	allowed_cfg.setValue("restrict", "c, d, e, f");
	unallowed_cfg.setValue("restrict", "d, e, f");

	// With no restrictions, everything should be allowed
	Restrict r1("");
	ensure(r1.is_allowed(""));
	ensure(r1.is_allowed("a,b,c"));
	ensure(r1.is_allowed(empty_cfg));
	ensure(r1.is_allowed(allowed_cfg));
	ensure(r1.is_allowed(unallowed_cfg));

	// Normal restrictions
	Restrict r2("a, b,c");
	ensure(not r2.is_allowed(""));
	ensure(r2.is_allowed("a"));
	ensure(r2.is_allowed("a, b"));
	ensure(r2.is_allowed("c, d, e, f"));
	ensure(not r2.is_allowed("d, e, f"));
	ensure(not r2.is_allowed(empty_cfg));
	ensure(r2.is_allowed(allowed_cfg));
	ensure(not r2.is_allowed(unallowed_cfg));
}

}

// vim:set ts=4 sw=4:
