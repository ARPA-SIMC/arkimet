/*
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/tests.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
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
using namespace wibble::tests;

struct arki_tests_shar {
};
TESTGRP(arki_tests);

// Check compareMaps
template<> template<>
void to::test<1>()
{
    wassert(actual(true).istrue());
    wassert(actual(false).isfalse());
    wassert(!actual(false).istrue());
    wassert(actual(3) == 3);
    wassert(!(actual(3) == 4));
    wassert(actual(3) != 4);
    wassert(!(actual(3) != 3));
    wassert(actual(3) < 4);
    wassert(!(actual(3) < 3));
    wassert(actual(3) <= 4);
    wassert(actual(3) <= 3);
    wassert(actual(4) > 3);
    wassert(!(actual(3) > 3));
    wassert(actual(4) >= 3);
    wassert(actual(3) >= 3);
    wassert(actual("ciao").startswith("ci"));
    wassert(!actual("ciao").startswith("ao"));
    wassert(actual("ciao").endswith("ao"));
    wassert(!actual("ciao").endswith("ci"));
    wassert(actual("ciao").contains("ci"));
    wassert(actual("ciao").contains("ia"));
    wassert(actual("ciao").contains("ao"));
    wassert(!actual("ciao").contains("bu"));
    wassert(actual("ciao").matches("[ia]+"));
    wassert(!actual("ciao").matches("[bu]+"));
    wassert(!actual("this-file-does-not-exist").fileexists());
}

}

// vim:set ts=4 sw=4:
