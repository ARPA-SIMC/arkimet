/*
 * Copyright (C) 2014--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include "typevector.h"

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_typevector_shar {
    arki_types_typevector_shar()
    {
    }
};
TESTGRP(arki_types_typevector);

def_test(1)
{
    TypeVector v;
    // Operations on empty
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0);
    wassert(actual(v == TypeVector()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());

    // Add a null and a valid entry
    v.set(1, decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));
    wassert(actual(v.empty()).isfalse());
    wassert(actual(v.size()) == 2);
    wassert(actual(v != TypeVector()).istrue());

    // Lookup
    wassert(actual(v[0]).isfalse());
    wassert(actual(v[1]).istrue());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 5, 12, 0, 0}));

    // Iteration
    {
        TypeVector::const_iterator i = v.begin();
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).isfalse());
        ++i;
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).is_reftime_position({2015, 1, 5, 12, 0, 0}));
        ++i;
        wassert(actual(i == v.end()).istrue());
    }

    // Replace a non-null entry
    v.set(1, decodeString(TYPE_REFTIME, "2015-01-06T12:00:00Z"));
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Replace a null entry
    v.set(0, decodeString(TYPE_REFTIME, "2015-01-04T12:00:00Z"));
    wassert(actual_type(v[0]).is_reftime_position({2015, 1, 4, 12, 0, 0}));

    // Unset
    v.unset(0);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Resize, enlarging
    v.resize(4);
    wassert(actual(v.size()) == 4);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
    wassert(actual_type(v[2]).isfalse());
    wassert(actual_type(v[3]).isfalse());

    // Resize, shrinking
    v.set(3, decodeString(TYPE_REFTIME, "2015-01-01T12:00:00Z"));
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
    wassert(actual_type(v[2]).isfalse());
    wassert(actual_type(v[3]).is_reftime_position({2015, 1, 1, 12, 0, 0}));
    v.resize(2);
    wassert(actual(v.size()) == 2);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Rtrim
    v.set(5, 0);
    wassert(actual(v.size()) == 6);
    v.rtrim();
    wassert(actual(v.size()) == 2);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Split
    TypeVector out;
    v.split(1, out);
    wassert(actual(v.size()) == 1);
    wassert(actual(out.size()) == 1);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(out[0]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
}

}

