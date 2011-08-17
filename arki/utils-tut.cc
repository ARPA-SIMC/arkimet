/*
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

#include <arki/tests/test-utils.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/types/origin.h>
#include <arki/types/run.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_shar {
};
TESTGRP(arki_utils);

// Check compareMaps
template<> template<>
void to::test<1>()
{
	using namespace utils;
	map<string, UItem<> > a;
	map<string, UItem<> > b;

	a["antani"] = b["antani"] = types::origin::GRIB1::create(1, 2, 3);
	a["pippo"] = types::run::Minute::create(12);

	ensure_equals(compareMaps(a, b), 1);
	ensure_equals(compareMaps(b, a), -1);
}

}

// vim:set ts=4 sw=4:
