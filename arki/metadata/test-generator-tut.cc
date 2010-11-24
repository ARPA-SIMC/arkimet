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
#include <arki/metadata.h>
#include <arki/metadata/test-generator.h>
#include <arki/metadata/collection.h>
#include <arki/types/origin.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::metadata;

struct arki_metadata_test_generator_shar {
    arki_metadata_test_generator_shar()
    {
    }
};
TESTGRP(arki_metadata_test_generator);

// Simple generation
template<> template<>
void to::test<1>()
{
    test::Generator g("grib1");
    Collection c;
    g.generate(c);
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::GRIB1);
}

template<> template<>
void to::test<2>()
{
    test::Generator g("grib2");
    Collection c;
    g.generate(c);
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::GRIB2);
}

template<> template<>
void to::test<3>()
{
    test::Generator g("bufr");
    Collection c;
    g.generate(c);
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::BUFR);
}

template<> template<>
void to::test<4>()
{
    test::Generator g("odimh5");
    Collection c;
    g.generate(c);
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::ODIMH5);
}

template<> template<>
void to::test<5>()
{
    test::Generator g("grib1");
    g.add(types::TYPE_ORIGIN, "GRIB1(98, 0, 10)");
    g.add(types::TYPE_ORIGIN, "GRIB1(200, 0, 10)");
    g.add(types::TYPE_ORIGIN, "GRIB1(80, 0, 10)");
    g.add(types::TYPE_PRODUCT, "GRIB1(98, 1, 22)");
    g.add(types::TYPE_PRODUCT, "GRIB1(98, 1, 33)");
    g.add(types::TYPE_LEVEL, "GRIB1(100, 0)");

    Collection c;
    g.generate(c);
    ensure_equals(c.size(), 6u);
}

}

// vim:set ts=4 sw=4:
