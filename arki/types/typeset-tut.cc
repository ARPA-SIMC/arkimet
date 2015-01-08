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
#include "typeset.h"

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_typeset_shar {
    arki_types_typeset_shar()
    {
    }
};
TESTGRP(arki_types_typeset);

template<> template<>
void to::test<1>()
{
    TypeSet v;
    // Operations on empty
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0);
    wassert(actual(v == TypeSet()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());

    // Add a null and a valid entry
    auto_ptr<Type> type(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));
    const Type* sample = type.get();
    wassert(actual(v.insert(type) == sample).istrue());
    wassert(actual(type.get()).isfalse());
    wassert(actual(v.empty()).isfalse());
    wassert(actual(v.size()) == 1);
    wassert(actual(v != TypeSet()).istrue());

    // Lookup
    wassert(actual(v.find(*decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z")) == sample).istrue());
    wassert(actual(v.find(*decodeString(TYPE_REFTIME, "2015-01-05T11:00:00Z")) == sample).isfalse());

    // Iteration
    {
        TypeSet::const_iterator i = v.begin();
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).is_reftime_position({2015, 1, 5, 12, 0, 0}));
        ++i;
        wassert(actual(i == v.end()).istrue());
    }

    // Insert again, we get the previous pointer
    wassert(actual(v.insert(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z")) == sample).istrue());

    // Unset, we are again empty
    v.erase(*sample);
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0);
    wassert(actual(v == TypeSet()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());
}

}

