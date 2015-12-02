#include "tests.h"
#include "reftime.h"
#include <arki/tests/lua.h>
#include <arki/matcher.h>
#include <arki/utils/string.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_reftime_shar {
};
TESTGRP(arki_types_reftime);

// Check Position
template<> template<>
void to::test<1>()
{
    arki::tests::TestGenericType t("reftime", "2015-01-02T03:04:05Z");
    t.lower.push_back("2014-01-01T00:00:00");
    t.higher.push_back("2015-01-03T00:00:00");
    // Period sort later than Position
    t.higher.push_back("2014-01-01T00:00:00 to 2014-01-31T00:00:00");
    t.exact_query = "=2015-01-02T03:04:05Z";
    wassert(t);

    unique_ptr<Reftime> o = Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2));
    wassert(actual(o).is_reftime_position(Time(2007, 6, 5, 4, 3, 2)));

    wassert(actual(o) == Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2)));
    wassert(actual(o) != Reftime::createPosition(Time(2007, 6, 5, 4, 3, 1)));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Period
template<> template<>
void to::test<2>()
{
    arki::tests::TestGenericType t("reftime", "2015-01-02T00:00:00Z to 2015-01-03T00:00:00Z");
    t.lower.push_back("2015-01-01T00:00:00");
    t.lower.push_back("2015-01-10T00:00:00");
    t.lower.push_back("2015-01-01T00:00:00 to 2015-01-03T12:00:00");
    t.higher.push_back("2015-01-02T12:00:00 to 2015-01-31T00:00:00");
//#warning This does not look like a query to match a period
//    t.exact_query = "=2007-06-05T04:03:02Z";
    wassert(t);

    unique_ptr<Reftime> o = Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3));
    wassert(actual(o).is_reftime_period(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

    wassert(actual(o) == Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));
    wassert(actual(o) != Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 3), Time(2008, 7, 6, 5, 4, 2)));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check range expansion
template<> template<>
void to::test<3>()
{
    unique_ptr<Time> begin;
    unique_ptr<Time> end;
    Time t1(2007, 6, 5, 4, 3, 2);
    Time t2(2008, 7, 6, 5, 4, 3);
    Time t3(2007, 7, 6, 5, 4, 3);
    Time t4(2009, 8, 7, 6, 5, 4);
    Time t5(2009, 7, 6, 5, 4, 3);
    Time t6(2010, 9, 8, 7, 6, 5);

    // Merge with position
    reftime::Position(t1).expand_date_range(begin, end);
    wassert(actual(begin) == t1);
    wassert(actual(end) == t1);

    // Merge with a second position
    reftime::Position(t2).expand_date_range(begin, end);
    wassert(actual(begin) == t1);
    wassert(actual(end) == t2);

    // Merge with a period
    reftime::Period(t3, t4).expand_date_range(begin, end);
    wassert(actual(begin) == t1);
    wassert(actual(end) == t4);
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#ifdef HAVE_LUA
    unique_ptr<Reftime> o = Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2));

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'POSITION' then return 'style is '..o.style..' instead of POSITION' end \n"
		"  t = o.time \n"
		"  if t.year ~= 2007 then return 't.year is '..t.year..' instead of 2007' end \n"
		"  if t.month ~= 6 then return 't.month is '..t.month..' instead of 6' end \n"
		"  if t.day ~= 5 then return 't.day is '..t.day..' instead of 5' end \n"
		"  if t.hour ~= 4 then return 't.hour is '..t.hour..' instead of 4' end \n"
		"  if t.minute ~= 3 then return 't.minute is '..t.minute..' instead of 3' end \n"
		"  if t.second ~= 2 then return 't.second is '..t.second..' instead of 2' end \n"
		"  if tostring(o) ~= '2007-06-05T04:03:02Z' then return 'tostring gave '..tostring(o)..' instead of 2007-06-05T04:03:02Z' end \n"
		"  o1 = arki_reftime.position(arki_time.time(2007, 6, 5, 4, 3, 2))\n"
		"  if o ~= o1 then return 'new reftime is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Reproduce bugs
template<> template<>
void to::test<5>()
{
    unique_ptr<Type> decoded = decodeString(TYPE_REFTIME, "2005-12-01T18:00:00Z");
    stringstream ss;
    ss << *decoded;
    wassert(actual(ss.str()) == "2005-12-01T18:00:00Z");
}

}
