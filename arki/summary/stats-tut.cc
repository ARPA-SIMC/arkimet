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
#include "stats.h"
#include <arki/metadata.h>
#include <wibble/sys/buffer.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;
using namespace wibble::tests;

struct arki_summary_stats_shar {
    Metadata md;

    arki_summary_stats_shar()
    {
        md.set(Reftime::createPosition(Time(2009, 8, 7, 6, 5, 4)));
        sys::Buffer buf;
        md.setInlineData("grib1", buf);
    }
};
TESTGRP(arki_summary_stats);

// Basic stats tests
template<> template<>
void to::test<1>()
{
    using namespace arki::summary;

    auto_ptr<Stats> st(new Stats);
    wassert(actual(st->count) == 0);
    wassert(actual(st->size) == 0);
    wassert(actual(*st) == Stats());

    st->merge(md);
    wassert(actual(st->count) == 1);
    wassert(actual(st->size) == 0);

    auto_ptr<Stats> st1(new Stats);
    st1->merge(md);
    st1->merge(md);

    wassert(actual(st).compares(*st1));
}

// Basic encode/decode tests
template<> template<>
void to::test<2>()
{
    using namespace arki::summary;

    auto_ptr<Stats> st(new Stats);
    st->merge(md);
    wassert(actual(st).serializes());
}

// Basic encode/decode tests with large numbers
template<> template<>
void to::test<3>()
{
    using namespace arki::summary;

    auto_ptr<Stats> st(new Stats);
    st->merge(md);
    st->count = 0x7FFFffffUL;
    st->size = 0x7FFFffffFFFFffffUL;
    wassert(actual(st).serializes());
}

}
