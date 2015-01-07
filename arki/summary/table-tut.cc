/*
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata.h>
#include "table.h"

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

struct arki_summary_table_shar {
    arki_summary_table_shar()
    {
    }
};
TESTGRP(arki_summary_table);

// Test basic operations
template<> template<>
void to::test<1>()
{
    Metadata md;
    md.set(decodeString(TYPE_ORIGIN, "GRIB1(98, 1, 2)"));
    md.set(decodeString(TYPE_PRODUCT, "GRIB1(98, 1, 2)"));
    md.set(decodeString(TYPE_TIMERANGE, "GRIB1(1)"));
    md.set(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));

    Table table;
    table.merge(md);
}

}
