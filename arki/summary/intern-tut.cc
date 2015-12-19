/*
 * Copyright (C) 2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/tests.h>
#include <arki/types/time.h>
#include "intern.h"

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

struct arki_summary_intern_shar {
    arki_summary_intern_shar()
    {
    }
};
TESTGRP(arki_summary_intern);

// Test basic operations
def_test(1)
{
    TypeIntern intern;

    const Type* val1 = intern.intern(Time(2015, 1, 5, 12, 0, 0));
    wassert(actual_type(val1).is_time(2015, 1, 5, 12, 0, 0));

    const Type* val2 = intern.intern(Time(2015, 1, 5, 12, 0, 0));
    wassert(actual(val1 == val2).istrue());

    val2 = intern.lookup(Time(2015, 1, 5, 12, 0, 0));
    wassert(actual(val1 == val2).istrue());

    val2 = intern.lookup(Time(2015, 1, 6, 12, 0, 0));
    wassert(actual(val2).isfalse());

    const Type* val3 = intern.intern(Time(2015, 1, 6, 12, 0, 0));
    wassert(actual_type(val3).is_time(2015, 1, 6, 12, 0, 0));
    wassert(actual(val1 != val2).istrue());
}

}
