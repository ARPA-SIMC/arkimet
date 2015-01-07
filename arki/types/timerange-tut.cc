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
#include <arki/types/timerange.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_timerange_shar {
};
TESTGRP(arki_types_timerange);

// Check GRIB1 with seconds
template<> template<>
void to::test<1>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 254, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB1, 002, 002s, 003s";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 254, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 254);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 2);
    wassert(actual(p2) == 3);
}

// Check GRIB1 with hours
template<> template<>
void to::test<2>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB1, 002, 002h, 003h";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 2 * 3600);
    wassert(actual(p2) == 3 * 3600);
}

// Check GRIB1 with years
template<> template<>
void to::test<3>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB1, 002, 002y, 003y";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 4, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 4);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::MONTH);
    wassert(actual(p1) == 2 * 12);
    wassert(actual(p2) == 3 * 12);
}

// Check GRIB1 with unknown values
template<> template<>
void to::test<4>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(250, 1, 124, 127)");
    tgt.lower.push_back("GRIB1(250, 1, 124, 126)");
    tgt.higher.push_back("GRIB1(250, 4, 124, 127)");
    tgt.higher.push_back("GRIB1(250, 254, 124, 127)");
    tgt.exact_query = "GRIB1, 250, 124h, 127h";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(250, 1, 124, 127);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 250);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 124);
    wassert(actual(v->p2()) == 127);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 124 * 3600);
    wassert(actual(p2) == 127 * 3600);
}

// Check GRIB2 with seconds
template<> template<>
void to::test<5>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 254, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB2,2,254,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 254, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 254);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with hours
template<> template<>
void to::test<6>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB2,2,1,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with years
template<> template<>
void to::test<7>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 3, 4)");
    tgt.lower.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB2,2,4,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 4);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with negative values
template<> template<>
void to::test<8>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 1, -2, -3)");
    tgt.lower.push_back("GRIB1(2, 4, -2, -3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, -2, -3)");
    tgt.exact_query = "GRIB2,2,1,-2,-3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, -2, -3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual((int)v->p1()) == -2);
    wassert(actual((int)v->p2()) == -3);
}

// Check GRIB2 with unknown values
template<> template<>
void to::test<9>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(250, 1, -2, -3)");
    tgt.lower.push_back("GRIB1(250, 4, -2, -3)");
    tgt.lower.push_back("GRIB1(250, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(250, 254, -2, -3)");
    tgt.exact_query = "GRIB2,250,1,-2,-3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(250, 1, -2, -3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 250);
    wassert(actual(v->unit()) == 1);
    wassert(actual((int)v->p1()) == -2);
    wassert(actual((int)v->p2()) == -3);
}

// Check GRIB2 with some values that used to fail
template<> template<>
void to::test<10>()
{
    auto_ptr<Timerange> o1 = Timerange::createGRIB2(11, 1, 3, 3);
    auto_ptr<Timerange> o2 = Timerange::createGRIB2(11, 1, 3, 6);
    const timerange::GRIB2* v1 = dynamic_cast<timerange::GRIB2*>(o1.get());
    const timerange::GRIB2* v2 = dynamic_cast<timerange::GRIB2*>(o2.get());
    wassert(actual(v1->type()) == 11u);
    wassert(actual(v1->unit()) == 1u);
    wassert(actual(v1->p1()) == 3u);
    wassert(actual(v1->p2()) == 3u);
    wassert(actual(v2->type()) == 11u);
    wassert(actual(v2->unit()) == 1u);
    wassert(actual(v2->p1()) == 3u);
    wassert(actual(v2->p2()) == 6u);

    wassert(actual(o1) != o2);

    wassert(actual(o1->encodeBinary()) != o2->encodeBinary());
    //ensure(o1 < o2);
    //ensure(o2 > o1);
}

// Check Timedef with step and statistical processing
template<> template<>
void to::test<11>()
{
    tests::TestGenericType tgt("timerange", "Timedef(6h,2,60m)");
    tgt.alternates.push_back("360m, 2, 1h");
    tgt.alternates.push_back("21600s,2,3600s");
    tgt.lower.push_back("Timedef(5h,2,60m)");
    tgt.lower.push_back("Timedef(6h)");
    tgt.lower.push_back("Timedef(6h,3,60m)");
    tgt.lower.push_back("Timedef(6h,2)");
    tgt.lower.push_back("Timedef(6h,2,61m)");
    tgt.lower.push_back("Timedef(6mo,2,60m)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, -2, -3)");
    tgt.exact_query = "Timedef,6h,2,60m";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("6h,2,60m");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_HOUR);
    wassert(actual(v->step_len()) == 6u);
    wassert(actual(v->stat_type()) == 2);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MINUTE);
    wassert(actual(v->stat_len()) == 60u);

    wassert(actual(v) == timerange::Timedef::create(6, timerange::UNIT_HOUR, 2, 60, timerange::UNIT_MINUTE));
}

// Check Timedef with step and statistical processing, in months
template<> template<>
void to::test<12>()
{
    tests::TestGenericType tgt("timerange", "Timedef(1y,2,3mo)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(6h,2)");
    tgt.lower.push_back("Timedef(6h,2,61m)");
    tgt.lower.push_back("Timedef(6mo,2,60m)");
    tgt.lower.push_back("Timedef(1y)");
    tgt.lower.push_back("Timedef(1y,3)");
    tgt.higher.push_back("Timedef(1y,3,3mo)");
    tgt.higher.push_back("Timedef(1y,2,4mo)");
    tgt.higher.push_back("Timedef(2y,2,3mo)");
    tgt.exact_query = "Timedef,1y,2,3mo";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("1y,2,3mo");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_YEAR);
    wassert(actual(v->step_len()) == 1u);
    wassert(actual(v->stat_type()) == 2);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MONTH);
    wassert(actual(v->stat_len()) == 3u);

    wassert(actual(v) == timerange::Timedef::create(1, timerange::UNIT_YEAR, 2, 3, timerange::UNIT_MONTH));
    wassert(actual(v) == timerange::Timedef::createFromYaml("12mo, 2, 3mo"));
}

// Check Timedef with step only
template<> template<>
void to::test<13>()
{
    tests::TestGenericType tgt("timerange", "Timedef(1d)");
    tgt.alternates.push_back("24h");
    tgt.alternates.push_back("1440m");
    tgt.alternates.push_back("86400s");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(6h,2)");
    tgt.higher.push_back("Timedef(2d)");
    tgt.higher.push_back("Timedef(1d,0)");
    tgt.higher.push_back("Timedef(1d,0,0s)");
    tgt.higher.push_back("Timedef(1m)");
    tgt.exact_query = "Timedef,1d,-";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("1d");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_DAY);
    wassert(actual(v->step_len()) == 1u);
    wassert(actual(v->stat_type()) == 255);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MISSING);
    wassert(actual(v->stat_len()) == 0u);

    wassert(actual(v) == timerange::Timedef::create(1, timerange::UNIT_DAY));
}

// Check Timedef with step only, in months
template<> template<>
void to::test<14>()
{
    tests::TestGenericType tgt("timerange", "Timedef(2ce)");
    tgt.alternates.push_back("20de");
    tgt.alternates.push_back("200y");
    tgt.alternates.push_back("2400mo");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(6h,2)");
    tgt.lower.push_back("Timedef(1ce)");
    tgt.higher.push_back("Timedef(2d)");
    tgt.higher.push_back("Timedef(1d,0)");
    tgt.higher.push_back("Timedef(2ce,0");
    tgt.higher.push_back("Timedef(2ce,0,0s");
    tgt.exact_query = "Timedef,2ce,-";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("2ce");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_CENTURY);
    wassert(actual(v->step_len()) == 2u);
    wassert(actual(v->stat_type()) == 255);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MISSING);
    wassert(actual(v->stat_len()) == 0u);

    wassert(actual(v) == timerange::Timedef::create(2, timerange::UNIT_CENTURY));
}

// Check Timedef2 with step, and only statistical process type
template<> template<>
void to::test<15>()
{
    tests::TestGenericType tgt("timerange", "Timedef(6h,2)");
    tgt.alternates.push_back("350m, 2");
    tgt.alternates.push_back("21600s,2");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(5h,2)");
    tgt.lower.push_back("Timedef(6h)");
    tgt.higher.push_back("Timedef(6h,3)");
    tgt.higher.push_back("Timedef(6h,2,60m)");
    tgt.higher.push_back("Timedef(2d)");
    tgt.exact_query = "Timedef,6h,2,-";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("6h,2");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_HOUR);
    wassert(actual(v->step_len()) == 6u);
    wassert(actual(v->stat_type()) == 2);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MISSING);
    wassert(actual(v->stat_len()) == 0u);

    wassert(actual(v) == timerange::Timedef::create(6, timerange::UNIT_HOUR, 2, 0, timerange::UNIT_MISSING));
}

// Check Timedef with step in months, and only statistical process type
template<> template<>
void to::test<16>()
{
    tests::TestGenericType tgt("timerange", "Timedef(6no,2)");
    tgt.alternates.push_back("180y, 2");
    tgt.alternates.push_back("18de, 2");
    tgt.alternates.push_back("2160mo, 2");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(5h,2)");
    tgt.lower.push_back("Timedef(6h)");
    tgt.lower.push_back("Timedef(5no,2)");
    tgt.lower.push_back("Timedef(6no)");
    tgt.higher.push_back("Timedef(6no,3)");
    tgt.higher.push_back("Timedef(6no,2,60d)");
    tgt.exact_query = "Timedef,6no,2,-";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("6no,2");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_NORMAL);
    wassert(actual(v->step_len()) == 6u);
    wassert(actual(v->stat_type()) == 2);
    wassert(actual(v->stat_unit()) == timerange::UNIT_MISSING);
    wassert(actual(v->stat_len()) == 0u);

    wassert(actual(v) == timerange::Timedef::create(6, timerange::UNIT_NORMAL, 2, 0, timerange::UNIT_MISSING));
}

// Check Timedef with step and statistical processing, one in seconds and one in months
template<> template<>
void to::test<17>()
{
    tests::TestGenericType tgt("timerange", "Timedef(1y,2,3d)");
    tgt.alternates.push_back("12mo,2,72h");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(5h,2)");
    tgt.lower.push_back("Timedef(6h)");
    tgt.lower.push_back("Timedef(1y)");
    tgt.lower.push_back("Timedef(1y,2)");
    tgt.higher.push_back("Timedef(1y,2,4d)");
    tgt.higher.push_back("Timedef(1y,3,3d)");
    tgt.higher.push_back("Timedef(2y,2,3d)");
    tgt.higher.push_back("Timedef(6no,3)");
    tgt.higher.push_back("Timedef(6no,2,60d)");
    tgt.exact_query = "Timedef,1y,2,3d";
    wassert(tgt);

    auto_ptr<timerange::Timedef> v = timerange::Timedef::createFromYaml("1y,2,3d");
    wassert(actual(v->style()) == Timerange::TIMEDEF);
    wassert(actual(v->step_unit()) == timerange::UNIT_YEAR);
    wassert(actual(v->step_len()) == 1u);
    wassert(actual(v->stat_type()) == 2);
    wassert(actual(v->stat_unit()) == timerange::UNIT_DAY);
    wassert(actual(v->stat_len()) == 3u);

    wassert(actual(v) == timerange::Timedef::create(1, timerange::UNIT_YEAR, 2, 3, timerange::UNIT_DAY));
}

// Check BUFR
template<> template<>
void to::test<18>()
{
    tests::TestGenericType tgt("timerange", "BUFR(6, 1)");
    tgt.alternates.push_back("360,0");
    tgt.alternates.push_back("21600,254");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 254, -2, -3)");
    tgt.lower.push_back("Timedef(5h,2)");
    tgt.lower.push_back("BUFR(5,1)");
    tgt.lower.push_back("BUFR(6,0)");
    tgt.higher.push_back("BUFR(7,1)");
    tgt.higher.push_back("BUFR(6,2)");
    tgt.higher.push_back("Timedef(1y,3,3d)");
    tgt.higher.push_back("Timedef(2y,2,3d)");
    tgt.higher.push_back("Timedef(6no,3)");
    tgt.higher.push_back("Timedef(6no,2,60d)");
    tgt.exact_query = "BUFR,6h";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createBUFR(6, 1);
    const timerange::BUFR* v = dynamic_cast<timerange::BUFR*>(o.get());
    wassert(actual(v->style()) == Timerange::BUFR);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->value()) == 6);
}


// Test Lua functions
template<> template<>
void to::test<19>()
{
#ifdef HAVE_LUA
    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 254, 2, 3);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  if o.type ~= 2 then return 'o.type is '..o.type..' instead of 2' end \n"
		"  if o.unit ~= 'second' then return 'o.unit is '..o.unit..' instead of \\'second\\'' end \n"
		"  if o.p1 ~= 2 then return 'o.p1 is '..o.p1..' instead of 2' end \n"
		"  if o.p2 ~= 3 then return 'o.p2 is '..o.p2..' instead of 3' end \n"
		"  if tostring(o) ~= 'GRIB1(002, 002s, 003s)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(002, 002s, 003s)' end \n"
		"  o1 = arki_timerange.grib1(2, 254, 2, 3)\n"
		"  if o ~= o1 then return 'new timerange is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Test computing timedef information
template<> template<>
void to::test<20>()
{
    int val;
    bool issec;

    {
        // GRIB1, forecast at +60min
        auto_ptr<Timerange> tr(Timerange::createGRIB1(0, 0, 60, 0));

        ensure(tr->get_forecast_step(val, issec));
        ensure_equals(val, 3600);
        ensure_equals(issec, true);

        ensure_equals(tr->get_proc_type(), 254);

        ensure(tr->get_proc_duration(val, issec));
        ensure_equals(val, 0);
        ensure_equals(issec, true);
    }

    {
        // GRIB1, average between rt+1h and rt+3h
        auto_ptr<Timerange> tr(Timerange::createGRIB1(3, 1, 1, 3));

        ensure(tr->get_forecast_step(val, issec));
        ensure_equals(val, 3 * 3600);
        ensure_equals(issec, true);

        ensure_equals(tr->get_proc_type(), 0);

        ensure(tr->get_proc_duration(val, issec));
        ensure_equals(val, 2 * 3600);
        ensure_equals(issec, true);
    }
}

// Test Timedef's validity_time_to_emission_time
template<> template<>
void to::test<21>()
{
    using namespace timerange;
    using namespace reftime;
    auto_ptr<Timedef> v = Timedef::createFromYaml("6h");
    auto_ptr<Position> p = downcast<Position>(Reftime::decodeString("2009-02-13 12:00:00"));
    auto_ptr<Position> p1 = v->validity_time_to_emission_time(*p);
    ensure_equals(p1->time.vals[0], 2009);
    ensure_equals(p1->time.vals[1], 2);
    ensure_equals(p1->time.vals[2], 13);
    ensure_equals(p1->time.vals[3], 6);
    ensure_equals(p1->time.vals[4], 0);
    ensure_equals(p1->time.vals[5], 0);
}

}

// vim:set ts=4 sw=4:

