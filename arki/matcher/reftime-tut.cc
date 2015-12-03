#include "config.h"
#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/matcher/reftime.h"
#include "arki/metadata.h"
#include "arki/configfile.h"
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_matcher_reftime_shar
{
    Metadata md;

    arki_matcher_reftime_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_matcher_reftime);

namespace {
std::string sql(const Matcher& m, const std::string& name)
{
    auto o = m.get(types::TYPE_REFTIME);
    if (!o) throw std::runtime_error("no reftime matcher found");
    return o->toReftimeSQL(name);
}
}

// Try matching reference times
template<> template<>
void to::test<1>()
{
	// md.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));

	ensure_matches("reftime:>=2007", md);
	ensure_equals(sql(Matcher::parse("reftime:>=2007"), "foo"), "(foo>='2007-01-01 00:00:00')");
	ensure_matches("reftime:<=2007", md);
	ensure_matches("reftime:>2006", md);
	ensure_matches("reftime:<2008", md);
	ensure_matches("reftime:==2007", md);
	ensure_matches("reftime:=2007", md);
	ensure_not_matches("reftime:>=2008", md);
	ensure_not_matches("reftime:<=2006", md);
	ensure_not_matches("reftime:>2007", md);
	ensure_not_matches("reftime:<2007", md);
	ensure_not_matches("reftime:==2006", md);
	ensure_equals(sql(Matcher::parse("reftime:==2006"), "foo"), "((foo>='2006-01-01 00:00:00' AND foo<='2006-12-31 23:59:59'))");

	ensure_matches("reftime:>=2007-01", md);
	ensure_matches("reftime:<=2007-01", md);
	ensure_matches("reftime:>2006-12", md);
	ensure_matches("reftime:<2007-02", md);
	ensure_matches("reftime:==2007-01", md);
	ensure_matches("reftime:=2007-01", md);
	ensure_not_matches("reftime:>=2007-02", md);
	ensure_not_matches("reftime:<=2006-12", md);
	ensure_not_matches("reftime:>2007-01", md);
	ensure_not_matches("reftime:<2007-01", md);
	ensure_not_matches("reftime:==2007-02", md);

	ensure_matches("reftime:>=2007-01-02", md);
	ensure_matches("reftime:<=2007-01-02", md);
	ensure_matches("reftime:>2007-01-01", md);
	ensure_matches("reftime:<2007-01-03", md);
	ensure_matches("reftime:==2007-01-02", md);
	ensure_matches("reftime:=2007-01-02", md);
	ensure_not_matches("reftime:>=2007-01-03", md);
	ensure_not_matches("reftime:<=2006-01-01", md);
	ensure_not_matches("reftime:>2007-01-02", md);
	ensure_not_matches("reftime:<2007-01-02", md);
	ensure_not_matches("reftime:==2007-01-01", md);

	ensure_matches("reftime:>=2007-01-02 03", md);
	ensure_matches("reftime:<=2007-01-02 03", md);
	ensure_matches("reftime:>2007-01-02 02", md);
	ensure_matches("reftime:<2007-01-02 04", md);
	ensure_matches("reftime:==2007-01-02 03", md);
	ensure_matches("reftime:=2007-01-02 03", md);
	ensure_not_matches("reftime:>=2007-01-02 04", md);
	ensure_not_matches("reftime:<=2006-01-02 02", md);
	ensure_not_matches("reftime:>2007-01-02 03", md);
	ensure_not_matches("reftime:<2007-01-02 03", md);
	ensure_not_matches("reftime:==2007-01-02 02", md);

	ensure_matches("reftime:>=2007-01-02 03:04", md);
	ensure_matches("reftime:<=2007-01-02 03:04", md);
	ensure_matches("reftime:>2007-01-02 03:03", md);
	ensure_matches("reftime:<2007-01-03 03:05", md);
	ensure_matches("reftime:==2007-01-02 03:04", md);
	ensure_matches("reftime:=2007-01-02 03:04", md);
	ensure_not_matches("reftime:>=2007-01-02 03:05", md);
	ensure_not_matches("reftime:<=2006-01-02 03:03", md);
	ensure_not_matches("reftime:>2007-01-02 03:04", md);
	ensure_not_matches("reftime:<2007-01-02 03:04", md);
	ensure_not_matches("reftime:==2007-01-02 03:03", md);

	ensure_matches("reftime:>=2007-01-02 03:04:05", md);
	ensure_matches("reftime:<=2007-01-02 03:04:05", md);
	ensure_matches("reftime:>2007-01-02 03:04:04", md);
	ensure_matches("reftime:<2007-01-03 03:04:06", md);
	ensure_matches("reftime:==2007-01-02 03:04:05", md);
	ensure_matches("reftime:=2007-01-02 03:04:05", md);
	ensure_not_matches("reftime:>=2007-01-02 03:04:06", md);
	ensure_not_matches("reftime:<=2006-01-02 03:04:04", md);
	ensure_not_matches("reftime:>2007-01-02 03:04:05", md);
	ensure_not_matches("reftime:<2007-01-02 03:04:05", md);
	ensure_not_matches("reftime:==2007-01-02 03:04:04", md);
}

// Try matching reference time intervals
template<> template<>
void to::test<2>()
{
    md.set(Reftime::createPeriod(Time(2007, 1, 2, 3, 4, 5), Time(2007, 2, 3, 4, 5, 6)));

	ensure_matches("reftime:>=2007", md);
	ensure_matches("reftime:<=2007", md);
	ensure_matches("reftime:>2006", md);
	ensure_matches("reftime:<2008", md);
	ensure_matches("reftime:==2007", md);
	ensure_matches("reftime:=2007", md);
	ensure_not_matches("reftime:>=2008", md);
	ensure_not_matches("reftime:<=2006", md);
	ensure_not_matches("reftime:>2007", md);
	ensure_not_matches("reftime:<2007", md);
	ensure_not_matches("reftime:==2006", md);

	ensure_matches("reftime:>=2007-01", md);
	ensure_matches("reftime:<=2007-01", md);
	ensure_matches("reftime:>2006-12", md);
	ensure_matches("reftime:<2007-02", md);
	ensure_matches("reftime:==2007-01", md);
	ensure_matches("reftime:=2007-01", md);
	ensure_matches("reftime:>=2007-02", md);
	ensure_not_matches("reftime:>=2007-03", md);
	ensure_not_matches("reftime:<=2006-12", md);
	ensure_matches("reftime:>2007-01", md);
	ensure_not_matches("reftime:>2007-02", md);
	ensure_not_matches("reftime:<2007-01", md);
	ensure_matches("reftime:==2007-02", md);
	ensure_not_matches("reftime:==2007-03", md);

	ensure_matches("reftime:>=2007-01-02", md);
	ensure_matches("reftime:<=2007-01-02", md);
	ensure_matches("reftime:>2007-01-01", md);
	ensure_matches("reftime:<2007-01-03", md);
	ensure_matches("reftime:==2007-01-02", md);
	ensure_matches("reftime:=2007-01-02", md);
	ensure_matches("reftime:>=2007-01-03", md);
	ensure_not_matches("reftime:>=2007-02-04", md);
	ensure_not_matches("reftime:<=2006-01-01", md);
	ensure_matches("reftime:>2007-01-02", md);
	ensure_not_matches("reftime:>2007-02-03", md);
	ensure_not_matches("reftime:<2007-01-02", md);
	ensure_not_matches("reftime:==2007-01-01", md);

	ensure_matches("reftime:>=2007-01-02 03", md);
	ensure_matches("reftime:<=2007-01-02 03", md);
	ensure_matches("reftime:>2007-01-02 02", md);
	ensure_matches("reftime:<2007-01-02 04", md);
	ensure_matches("reftime:==2007-01-02 03", md);
	ensure_matches("reftime:=2007-01-02 03", md);
	ensure_matches("reftime:>=2007-01-02 04", md);
	ensure_not_matches("reftime:>=2007-02-03 05", md);
	ensure_not_matches("reftime:<=2006-01-02 02", md);
	ensure_matches("reftime:>2007-01-02 03", md);
	ensure_not_matches("reftime:>2007-02-03 04", md);
	ensure_not_matches("reftime:<2007-01-02 03", md);
	ensure_not_matches("reftime:==2007-01-02 02", md);

	ensure_matches("reftime:>=2007-01-02 03:04", md);
	ensure_matches("reftime:<=2007-01-02 03:04", md);
	ensure_matches("reftime:>2007-01-02 03:03", md);
	ensure_matches("reftime:<2007-01-03 03:05", md);
	ensure_matches("reftime:==2007-01-02 03:04", md);
	ensure_matches("reftime:=2007-01-02 03:04", md);
	ensure_matches("reftime:>=2007-01-02 03:05", md);
	ensure_not_matches("reftime:>=2007-02-03 04:06", md);
	ensure_not_matches("reftime:<=2006-01-02 03:03", md);
	ensure_matches("reftime:>2007-01-02 03:04", md);
	ensure_not_matches("reftime:>2007-02-03 04:05", md);
	ensure_not_matches("reftime:<2007-01-02 03:04", md);
	ensure_not_matches("reftime:==2007-01-02 03:03", md);

	ensure_matches("reftime:>=2007-01-02 03:04:05", md);
	ensure_matches("reftime:<=2007-01-02 03:04:05", md);
	ensure_matches("reftime:>2007-01-02 03:04:04", md);
	ensure_matches("reftime:<2007-01-03 03:04:06", md);
	ensure_matches("reftime:==2007-01-02 03:04:05", md);
	ensure_matches("reftime:=2007-01-02 03:04:05", md);
	ensure_matches("reftime:>=2007-01-02 03:04:06", md);
	ensure_not_matches("reftime:>=2007-02-03 04:05:07", md);
	ensure_not_matches("reftime:<=2006-01-02 03:04:04", md);
	ensure_matches("reftime:>2007-01-02 03:04:05", md);
	ensure_not_matches("reftime:>2007-02-03 04:05:06", md);
	ensure_not_matches("reftime:<2007-01-02 03:04:05", md);
	ensure_not_matches("reftime:==2007-01-02 03:04:04", md);
}

// Try matching open ended reference time intervals
template<> template<>
void to::test<3>()
{
    // FIXME: are open ended Period reftimes still supported at all?
#if 0
    md.set(Reftime::createPeriod(Time(2007, 1, 2, 3, 4, 5), Time()));

	ensure_matches("reftime:>=2007", md);
	ensure_matches("reftime:<=2007", md);
	ensure_matches("reftime:>2006", md);
	ensure_matches("reftime:<2008", md);
	ensure_matches("reftime:==2007", md);
	ensure_matches("reftime:=2007", md);
	ensure_matches("reftime:>=2008", md);
	ensure_not_matches("reftime:<=2006", md);
	ensure_matches("reftime:>2007", md);
	ensure_not_matches("reftime:<2007", md);
	ensure_not_matches("reftime:==2006", md);
	ensure_matches("reftime:==2020", md);
	// It'll be a long while since we'll have a Y2K-like problem here
	ensure_matches("reftime:==9999", md);
#endif
}

// Special cases that caused bugs
template<> template<>
void to::test<4>()
{
    Metadata md;

    md.set(Reftime::createPosition(Time(2007, 1, 1, 0, 0, 0)));
    ensure_matches("reftime:>2006", md);
    //ensure_equals(Matcher::parse("reftime:>2006").toString(), "reftime:>2006-12-31 23:59:59");
}

// Try matching times
template<> template<>
void to::test<5>()
{
	// 2007-01-02 03:04:05

	ensure_matches("reftime:=03", md);
	ensure_not_matches("reftime:=02", md);
	ensure_matches("reftime:==03", md);
	ensure_not_matches("reftime:==02", md);
	ensure_matches("reftime:>=03", md);
	ensure_matches("reftime:>=02", md);
	ensure_not_matches("reftime:>=04", md);
	ensure_matches("reftime:<=03", md);
	ensure_matches("reftime:<=04", md);
	ensure_not_matches("reftime:<=02", md);
	ensure_matches("reftime:>02", md);
	ensure_matches("reftime:<04", md);
	ensure_not_matches("reftime:>03", md);
	ensure_equals(sql(Matcher::parse("reftime:>03"), "foo"), "(TIME(foo)>'03:59:59')");
	ensure_not_matches("reftime:<03", md);
	ensure_equals(sql(Matcher::parse("reftime:<03"), "foo"), "(TIME(foo)<'03:00:00')");

	ensure_matches("reftime:=03:04", md);
	ensure_not_matches("reftime:=03:03", md);
	ensure_matches("reftime:==03:04", md);
	ensure_not_matches("reftime:==03:03", md);
	ensure_matches("reftime:>=03:04", md);
	ensure_matches("reftime:>=03:03", md);
	ensure_not_matches("reftime:>=03:05", md);
	ensure_matches("reftime:<=03:04", md);
	ensure_matches("reftime:<=03:05", md);
	ensure_not_matches("reftime:<=03:03", md);
	ensure_matches("reftime:>03:03", md);
	ensure_matches("reftime:<03:05", md);
	ensure_not_matches("reftime:>03:04", md);
	ensure_equals(sql(Matcher::parse("reftime:>03:04"), "foo"), "(TIME(foo)>'03:04:59')");
	ensure_not_matches("reftime:<03:04", md);
	ensure_equals(sql(Matcher::parse("reftime:<03:04"), "foo"), "(TIME(foo)<'03:04:00')");

	ensure_matches("reftime:=03:04:05", md);
	ensure_not_matches("reftime:=03:04:04", md);
	ensure_matches("reftime:==03:04:05", md);
	ensure_not_matches("reftime:==03:04:04", md);
	ensure_matches("reftime:>=03:04:05", md);
	ensure_matches("reftime:>=03:04:04", md);
	ensure_not_matches("reftime:>=03:04:06", md);
	ensure_matches("reftime:<=03:04:05", md);
	ensure_matches("reftime:<=03:04:06", md);
	ensure_not_matches("reftime:<=03:04:04", md);
	ensure_matches("reftime:>03:04:04", md);
	ensure_matches("reftime:<03:04:06", md);
	ensure_not_matches("reftime:>03:04:05", md);
	ensure_equals(sql(Matcher::parse("reftime:>03:04:05"), "foo"), "(TIME(foo)>'03:04:05')");
	ensure_not_matches("reftime:<03:04:05", md);
	ensure_equals(sql(Matcher::parse("reftime:<03:04:05"), "foo"), "(TIME(foo)<'03:04:05')");
}

// Try matching time repetitions
template<> template<>
void to::test<6>()
{
	// 2007-01-02 03:04:05
	ensure_matches("reftime:>2007-01-02 00:04:05%3h", md);
	ensure_matches("reftime:<2007-01-02 06:04:05%3h", md);
	ensure_not_matches("reftime:>2007-01-02 00:04:05%6h", md);
	ensure_equals(sql(Matcher::parse("reftime:>2007-01-02 00:04:05%6h"), "foo"), "(foo>'2007-01-02 00:04:05' AND (TIME(foo)=='00:04:05' OR TIME(foo)=='06:04:05' OR TIME(foo)=='12:04:05' OR TIME(foo)=='18:04:05'))");
}

// Try matching times
template<> template<>
void to::test<7>()
{
    Matcher m;
    md.set(Reftime::createPeriod(Time(2007, 1, 2, 9, 0, 0), Time(2007, 1, 2, 15, 0, 0)));

	ensure_not_matches("reftime:==00:00:00", md);
	ensure_matches("reftime:==12:00:00", md);
	ensure_not_matches("reftime:==18:00:00", md);

	ensure_matches("reftime:>12", md);
	ensure_matches("reftime:<12", md);
	ensure_not_matches("reftime:>15", md);
	ensure_not_matches("reftime:<9", md);
	ensure_matches("reftime:>=15", md);
	ensure_matches("reftime:<=9", md);

	ensure_matches("reftime:>=00:00:00%12h", md);
	ensure_not_matches("reftime:>=00:00:00%18h", md);
}

// Try matching times
template<> template<>
void to::test<8>()
{
    Matcher m;
    md.set(Reftime::createPeriod(Time(2007, 1, 2, 21, 0, 0), Time(2007, 1, 3, 3, 0, 0)));

	ensure_matches("reftime:==00:00:00", md);
	ensure_not_matches("reftime:==12:00:00", md);
	ensure_not_matches("reftime:==18:00:00", md);
	ensure_matches("reftime:==21:00:00", md);

	ensure_matches("reftime:>12", md);
	ensure_matches("reftime:<12", md);
	ensure_matches("reftime:>15", md);
	ensure_matches("reftime:<9", md);
	ensure_matches("reftime:>=15", md);
	ensure_matches("reftime:<=9", md);

	ensure_matches("reftime:==00:00:00%12h", md);
	ensure_not_matches("reftime:==06:00:00%12h", md);
}

}

// vim:set ts=4 sw=4:
