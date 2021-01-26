#include "arki/core/tests.h"
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;
using arki::core::Interval;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_core_time");

static const int _ = 0;

Interval interval(int begin, int end)
{
    return Interval(begin == 0 ? Time() : Time(begin, 1), end == 0 ? Time() : Time(end, 1));
}


void Tests::register_tests() {

// Check 'now' time
add_method("now", [] {
    Time o(0, 0, 0);
    wassert(actual(o).is(0, 0, 0, 0, 0, 0));
    wassert(actual(o) == Time(0, 0, 0));
    wassert(actual(o) == Time(0, 0, 0, 0, 0, 0));
    wassert(actual(o) != Time(1789, 7, 14, 12, 0, 0));

    // Test serialisation to ISO-8601
    wassert(actual(o.to_iso8601()) == "0000-00-00T00:00:00Z");

    // Test serialisation to SQL
    wassert(actual(o.to_sql()) == "0000-00-00 00:00:00");

    // Test deserialisation from ISO-8601
    wassert(actual(o) == Time::create_iso8601(o.to_iso8601()));

    // Test deserialisation from SQL
    wassert(actual(o) == Time::create_sql(o.to_sql()));

    wassert(actual(o).serializes());
});

// Check comparison of times
add_method("compare", [] {
    wassert(actual(Time(1, 2).compare(Time(1, 2))) == 0);
    wassert(actual(Time(1, 2).compare(Time(1, 3))) < 0);
    wassert(actual(Time(1, 2).compare(Time(1, 1))) > 0);

    wassert(actual(Time(1, 2, 3).compare(Time(1, 2, 3))) == 0);
    wassert(actual(Time(1, 2, 3).compare(Time(1, 2, 4))) < 0);
    wassert(actual(Time(1, 2, 3).compare(Time(1, 2, 2))) > 0);

    wassert(actual(Time(1, 2, 3, 4).compare(Time(1, 2, 3, 4))) == 0);
    wassert(actual(Time(1, 2, 3, 4).compare(Time(1, 2, 3, 5))) < 0);
    wassert(actual(Time(1, 2, 3, 4).compare(Time(1, 2, 3, 3))) > 0);

    wassert(actual(Time(1, 2, 3, 4, 5).compare(Time(1, 2, 3, 4, 5))) == 0);
    wassert(actual(Time(1, 2, 3, 4, 5).compare(Time(1, 2, 3, 4, 6))) < 0);
    wassert(actual(Time(1, 2, 3, 4, 5).compare(Time(1, 2, 3, 4, 4))) > 0);

    wassert(actual(Time(1, 2, 3, 4, 5, 6).compare(Time(1, 2, 3, 4, 5, 6))) == 0);
    wassert(actual(Time(1, 2, 3, 4, 5, 6).compare(Time(1, 2, 3, 4, 5, 7))) < 0);
    wassert(actual(Time(1, 2, 3, 4, 5, 6).compare(Time(1, 2, 3, 4, 5, 5))) > 0);
});

// Check an arbitrary time
add_method("arbitrary", [] {
    Time o = Time(1, 2, 3, 4, 5, 6);
    wassert(actual(o).is(1, 2, 3, 4, 5, 6));

    wassert(actual(o) == Time(1, 2, 3, 4, 5, 6));
    wassert(actual(o) != Time(1789, 7, 14, 12, 0, 0));
    wassert(actual(o.compare(Time(1, 2, 3, 4, 5, 6))) == 0);
    wassert(actual(o.compare(Time(1, 2, 3, 4, 5, 7))) < 0);
    wassert(actual(o.compare(Time(1, 2, 3, 4, 5, 5))) > 0);

    // Test serialisation to ISO-8601
    wassert(actual(o.to_iso8601()) == "0001-02-03T04:05:06Z");

    // Test serialisation to SQL
    wassert(actual(o.to_sql()) == "0001-02-03 04:05:06");

    // Test deserialisation from ISO-8601
    wassert(actual(o) == Time::create_iso8601(o.to_iso8601()));

    // Test deserialisation from SQL
    wassert(actual(o) == Time::create_sql(o.to_sql()));

    wassert(actual(o).serializes());
});

// Test Time::generate
add_method("generate", [] {
    vector<Time> v = Time::generate(Time(2009, 1, 1, 0, 0, 0), Time(2009, 2, 1, 0, 0, 0), 3600);
    wassert(actual(v.size()) == 31u*24u);
});

// Test Time::range_overlaps
add_method("range_overlaps", [] {
    unique_ptr<Time> topen;
    unique_ptr<Time> t2000(new Time(2000, 1, 1, 0, 0, 0));
    unique_ptr<Time> t2005(new Time(2005, 1, 1, 0, 0, 0));
    unique_ptr<Time> t2007(new Time(2007, 1, 1, 0, 0, 0));
    unique_ptr<Time> t2010(new Time(2010, 1, 1, 0, 0, 0));

    wassert(actual(Time::range_overlaps(topen.get(), topen.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), topen.get(), t2005.get(), t2010.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), t2010.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), topen.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), topen.get(), t2010.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), t2005.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), t2005.get(), topen.get(), t2010.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), t2010.get(), topen.get(), t2005.get())).istrue());
    // End extreme excluded
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), topen.get(), t2010.get())).isfalse());
    wassert(actual(Time::range_overlaps(t2000.get(), t2005.get(), t2007.get(), t2010.get())).isfalse());
    wassert(actual(Time::range_overlaps(t2007.get(), t2010.get(), t2000.get(), t2005.get())).isfalse());
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), topen.get(), t2005.get())).isfalse());
});

add_method("to_unix", [] {
    Time t = Time::decodeString("2005-12-01T18:00:00Z");
    wassert(actual(t.to_unix()) == 1133460000);

    t = Time::decodeString("2005-12-01T18:01:02Z");
    wassert(actual(t.to_unix()) == 1133460062);
});

// Reproduce bugs
add_method("regression1", [] {
    Time decoded = Time::decodeString("2005-12-01T18:00:00Z");
    stringstream ss;
    ss << decoded;
    wassert(actual(ss.str()) == "2005-12-01T18:00:00Z");
});

add_method("interval_contains_point", [] {
    wassert_true(interval(_, _).contains(Time(2000, 1)));

    wassert_true(interval(2000, _).contains(Time(2000, 1)));
    wassert_true(interval(2000, _).contains(Time(2010, 1)));
    wassert_false(interval(2000, _).contains(Time(1999, 1)));
    wassert_false(interval(2000, _).contains(Time(1999, 12, 31, 23, 59, 59)));

    wassert_false(interval(_, 2000).contains(Time(2000, 1)));
    wassert_false(interval(_, 2000).contains(Time(2010, 1)));
    wassert_true(interval(_, 2000).contains(Time(1999, 12, 31, 23, 59, 59)));
    wassert_true(interval(_, 2000).contains(Time(1999, 1)));
});

add_method("interval_contains_range", [] {
    wassert_true(interval(_, _).contains(interval(_, _)));
    wassert_true(interval(_, _).contains(interval(2000, _)));
    wassert_true(interval(_, _).contains(interval(_, 2000)));
    wassert_true(interval(_, _).contains(interval(2000, 2010)));

    wassert_true(interval(_, 2000).contains(interval(1995, 1998)));
    wassert_true(interval(_, 2000).contains(interval(1995, 2000)));
    wassert_true(interval(_, 2000).contains(interval(_, 1998)));
    wassert_true(interval(_, 2000).contains(interval(_, 2000)));
    wassert_false(interval(_, 2000).contains(interval(2000, 2000)));
    wassert_false(interval(_, 2000).contains(interval(_, 2001)));
    wassert_false(interval(_, 2000).contains(interval(1998, 2001)));
    wassert_false(interval(_, 2000).contains(interval(2001, _)));
    wassert_false(interval(_, 2000).contains(interval(_, _)));

    wassert_true(interval(2000, _).contains(interval(2005, 2006)));
    wassert_true(interval(2000, _).contains(interval(2000, 2005)));
    wassert_true(interval(2000, _).contains(interval(2005, _)));
    wassert_true(interval(2000, _).contains(interval(2000, _)));
    wassert_false(interval(2000, _).contains(interval(2000, 2000)));
    wassert_false(interval(2000, _).contains(interval(1999, 2000)));
    wassert_false(interval(2000, _).contains(interval(1999, _)));
    wassert_false(interval(2000, _).contains(interval(1998, 2001)));
    wassert_false(interval(2000, _).contains(interval(_, 2001)));
    wassert_false(interval(2000, _).contains(interval(_, _)));

    wassert_true(interval(2000, 2010).contains(interval(2000, 2010)));
    wassert_true(interval(2000, 2010).contains(interval(2000, 2005)));
    wassert_true(interval(2000, 2010).contains(interval(2005, 2010)));
    wassert_true(interval(2000, 2010).contains(interval(2005, 2006)));

    wassert_false(interval(2000, 2010).contains(interval(_, 1995)));
    wassert_false(interval(2000, 2010).contains(interval(_, 2000)));
    wassert_false(interval(2000, 2010).contains(interval(_, 2005)));
    wassert_false(interval(2000, 2010).contains(interval(_, 2010)));
    wassert_false(interval(2000, 2010).contains(interval(_, 2015)));
    wassert_false(interval(2000, 2010).contains(interval(1995, _)));
    wassert_false(interval(2000, 2010).contains(interval(2000, _)));
    wassert_false(interval(2000, 2010).contains(interval(2005, _)));
    wassert_false(interval(2000, 2010).contains(interval(2010, _)));
    wassert_false(interval(2000, 2010).contains(interval(2015, _)));
    wassert_false(interval(2000, 2010).contains(interval(_, _)));
    wassert_false(interval(2000, 2010).contains(interval(2000, 2011)));
    wassert_false(interval(2000, 2010).contains(interval(1999, 2010)));
    wassert_false(interval(2000, 2010).contains(interval(2000, 2000)));
    wassert_false(interval(2000, 2010).contains(interval(2010, 2010)));
    wassert_false(interval(2000, 2010).contains(interval(1995, 2015)));
});

add_method("interval_intersects", [] {
    wassert_true(interval(_, _).intersects(interval(_, _)));
    wassert_true(interval(_, _).intersects(interval(2000, _)));
    wassert_true(interval(_, _).intersects(interval(_, 2000)));
    wassert_true(interval(_, _).intersects(interval(2000, 2010)));

    wassert_true(interval(_, 2000).intersects(interval(1995, 1998)));
    wassert_true(interval(_, 2000).intersects(interval(1995, 2000)));
    wassert_true(interval(_, 2000).intersects(interval(_, 1998)));
    wassert_true(interval(_, 2000).intersects(interval(_, 2000)));
    wassert_true(interval(_, 2000).intersects(interval(_, 2001)));
    wassert_true(interval(_, 2000).intersects(interval(1998, 2001)));
    wassert_true(interval(_, 2000).intersects(interval(_, _)));
    wassert_false(interval(_, 2000).intersects(interval(2000, 2000)));
    wassert_false(interval(_, 2000).intersects(interval(2000, 2001)));
    wassert_false(interval(_, 2000).intersects(interval(2001, 2002)));
    wassert_false(interval(_, 2000).intersects(interval(2000, _)));
    wassert_false(interval(_, 2000).intersects(interval(2001, _)));

    wassert_true(interval(2000, _).intersects(interval(2005, 2006)));
    wassert_true(interval(2000, _).intersects(interval(2000, 2005)));
    wassert_true(interval(2000, _).intersects(interval(1999, _)));
    wassert_true(interval(2000, _).intersects(interval(2000, _)));
    wassert_true(interval(2000, _).intersects(interval(2005, _)));
    wassert_true(interval(2000, _).intersects(interval(1998, 2001)));
    wassert_true(interval(2000, _).intersects(interval(_, 2001)));
    wassert_true(interval(2000, _).intersects(interval(_, _)));
    wassert_false(interval(2000, _).intersects(interval(1998, 1999)));
    wassert_false(interval(2000, _).intersects(interval(1999, 2000)));
    wassert_false(interval(2000, _).intersects(interval(2000, 2000)));
    wassert_false(interval(2000, _).intersects(interval(_, 2000)));
    wassert_false(interval(2000, _).intersects(interval(_, 1997)));

    wassert_true(interval(2000, 2010).intersects(interval(2000, 2010)));
    wassert_true(interval(2000, 2010).intersects(interval(2000, 2005)));
    wassert_true(interval(2000, 2010).intersects(interval(2005, 2010)));
    wassert_true(interval(2000, 2010).intersects(interval(2005, 2006)));

    wassert_false(interval(2000, 2010).intersects(interval(_, 1995)));
    wassert_false(interval(2000, 2010).intersects(interval(_, 2000)));
    wassert_true(interval(2000, 2010).intersects(interval(_, 2005)));
    wassert_true(interval(2000, 2010).intersects(interval(_, 2010)));
    wassert_true(interval(2000, 2010).intersects(interval(_, 2015)));
    wassert_true(interval(2000, 2010).intersects(interval(1995, _)));
    wassert_true(interval(2000, 2010).intersects(interval(2000, _)));
    wassert_true(interval(2000, 2010).intersects(interval(2005, _)));
    wassert_false(interval(2000, 2010).intersects(interval(2010, _)));
    wassert_false(interval(2000, 2010).intersects(interval(2015, _)));
    wassert_true(interval(2000, 2010).intersects(interval(_, _)));
    wassert_true(interval(2000, 2010).intersects(interval(2000, 2011)));
    wassert_true(interval(2000, 2010).intersects(interval(1999, 2010)));
    wassert_false(interval(2000, 2010).intersects(interval(2000, 2000)));
    wassert_false(interval(2000, 2010).intersects(interval(2010, 2010)));
    wassert_true(interval(2000, 2010).intersects(interval(1995, 2015)));
});

add_method("spans_one_whole_month", [] {
    wassert_true(interval(_, _).spans_one_whole_month());
    wassert_true(interval(_, 2000).spans_one_whole_month());
    wassert_true(interval(2000, _).spans_one_whole_month());
    wassert_true(interval(2000, 2001).spans_one_whole_month());

    wassert_true(Interval(Time(2000, 1, 1), Time(2000, 2, 1)).spans_one_whole_month());
    wassert_true(Interval(Time(2000, 1, 1), Time(2000, 2, 15)).spans_one_whole_month());
    wassert_true(Interval(Time(1999, 12, 15), Time(2000, 2, 1)).spans_one_whole_month());
    wassert_true(Interval(Time(1999, 12, 15), Time(2000, 2, 15)).spans_one_whole_month());

    wassert_false(interval(2000, 2000).spans_one_whole_month());

    wassert_false(Interval(Time(2000, 1, 1), Time(2000, 1, 31)).spans_one_whole_month());
    wassert_false(Interval(Time(2000, 1, 1), Time(2000, 1, 31, 23, 59, 59)).spans_one_whole_month());
});

// Test intersection
add_method("interval_intersection", [] {
    Interval test;

    auto intersect = [&](int begin, int end)
    {
        Interval res = test;
        wassert_true(res.intersect(interval(begin, end)));
        return actual(res);
    };

    auto intersect_fails = [&](int begin, int end)
    {
        Interval res = test;
        wassert_false(res.intersect(interval(begin, end)));
        wassert(actual(res) == test);
    };

    // Test intersection with interval open on both ends
    wassert(intersect(2000, _) == interval(2000, _));
    wassert(intersect(_, 2000) == interval(_, 2000));
    wassert(intersect(1999, 2000) == interval(1999, 2000));


    // Test intersection with interval open at end
    test = interval(2000, _);

    // Disjoint
    wassert(intersect_fails(_, 1999));
    wassert(intersect_fails(_, 2000));
    wassert(intersect_fails(1999, 2000));

    // Fully open ended
    wassert(intersect(_, _) == interval(2000, _));

    // Self
    wassert(intersect(2000, _) == interval(2000, _));
    wassert(intersect(1999, _) == interval(2000, _));
    wassert(intersect(2001, _) == interval(2001, _));
    wassert(intersect(_, 2001) == interval(2000, 2001));
    wassert(intersect(1999, 2001) == interval(2000, 2001));
    wassert(intersect(2001, 2002) == interval(2001, 2002));


    // Test intersection with interval open at begin
    test = interval(_, 2000);

    // Disjoint
    wassert(intersect_fails(2000, _));
    wassert(intersect_fails(2001, _));
    wassert(intersect_fails(2000, 2001));

    // Fully open ended
    wassert(intersect(_, _) == interval(_, 2000));

    // Self
    wassert(intersect(_, 2000) == interval(_, 2000));
    wassert(intersect(_, 1999) == interval(_, 1999));
    wassert(intersect(_, 2001) == interval(_, 2000));
    wassert(intersect(1999, _) == interval(1999, 2000));
    wassert(intersect(1999, 2001) == interval(1999, 2000));
    wassert(intersect(1998, 1999) == interval(1998, 1999));


    // Test intersection with interval closed at both ends
    test = interval(2000, 2010);

    // Disjoint
    wassert(intersect_fails(2010, _));
    wassert(intersect_fails(2011, _));
    wassert(intersect_fails(_, 2000));
    wassert(intersect_fails(_, 1999));
    wassert(intersect_fails(1990, 1995));
    wassert(intersect_fails(2015, 2020));

    // Fully open ended
    wassert(intersect(_, _) == interval(2000, 2010));

    // Self
    wassert(intersect(2000, 2010) == interval(2000, 2010));
    wassert(intersect(2000, _) == interval(2000, 2010));
    wassert(intersect(_, 2010) == interval(2000, 2010));
    wassert(intersect(_, 2001) == interval(2000, 2001));
    wassert(intersect(2009, _) == interval(2009, 2010));
    wassert(intersect(2000, 2009) == interval(2000, 2009));
    wassert(intersect(2001, 2010) == interval(2001, 2010));
    wassert(intersect(2001, 2009) == interval(2001, 2009));
    wassert(intersect(1995, 2010) == interval(2000, 2010));
    wassert(intersect(2000, 2015) == interval(2000, 2010));
    wassert(intersect(1995, 2005) == interval(2000, 2005));
    wassert(intersect(2005, 2015) == interval(2005, 2010));
    wassert(intersect(1995, 2015) == interval(2000, 2010));
});

add_method("interval_extend", [] {
    Interval test;

    auto extend = [&](int begin, int end)
    {
        Interval res = test;
        res.extend(interval(begin, end));
        return actual(res);
    };

    // Test extend with interval open on both ends
    test = Interval();
    wassert(extend(_, _) == Interval());
    wassert(extend(2000, _) == Interval());
    wassert(extend(_, 2000) == Interval());
    wassert(extend(1999, 2000) == Interval());

    // Test intersection with interval open at end
    test = interval(2000, _);

    // Disjoint
    wassert(extend(_, 1999) == Interval());
    wassert(extend(_, 2000) == Interval());
    wassert(extend(1999, 2000) == interval(1999, _));

    // Fully open ended
    wassert(extend(_, _) == Interval());

    // Self
    wassert(extend(2000, _) == interval(2000, _));
    wassert(extend(1999, _) == interval(1999, _));
    wassert(extend(2001, _) == interval(2000, _));
    wassert(extend(_, 2001) == Interval());
    wassert(extend(1999, 2001) == interval(1999, _));
    wassert(extend(2001, 2002) == interval(2000, _));


    // Test intersection with interval open at begin
    test = interval(_, 2000);

    // Disjoint
    wassert(extend(2000, _) == Interval());
    wassert(extend(2001, _) == Interval());
    wassert(extend(2000, 2001) == interval(_, 2001));
    wassert(extend(2001, 2002) == interval(_, 2002));

    // Fully open ended
    wassert(extend(_, _) == Interval());

    // Self
    wassert(extend(_, 2000) == interval(_, 2000));
    wassert(extend(_, 1999) == interval(_, 2000));
    wassert(extend(_, 2001) == interval(_, 2001));
    wassert(extend(1999, _) == Interval());
    wassert(extend(1999, 2001) == interval(_, 2001));
    wassert(extend(1998, 1999) == interval(_, 2000));


    // Test intersection with interval closed at both ends
    test = interval(2000, 2010);

    // Disjoint
    wassert(extend(2010, _) == interval(2000, _));
    wassert(extend(2011, _) == interval(2000, _));
    wassert(extend(_, 2000) == interval(_, 2010));
    wassert(extend(_, 1999) == interval(_, 2010));
    wassert(extend(1990, 1995) == interval(1990, 2010));
    wassert(extend(2015, 2020) == interval(2000, 2020));

    // Fully open ended
    wassert(extend(_, _) == Interval());

    // Self
    wassert(extend(2000, 2010) == interval(2000, 2010));
    wassert(extend(2000, _) == interval(2000, _));
    wassert(extend(_, 2010) == interval(_, 2010));
    wassert(extend(_, 2001) == interval(_, 2010));
    wassert(extend(2009, _) == interval(2000, _));
    wassert(extend(2000, 2009) == interval(2000, 2010));
    wassert(extend(2001, 2010) == interval(2000, 2010));
    wassert(extend(2001, 2009) == interval(2000, 2010));
    wassert(extend(1995, 2010) == interval(1995, 2010));
    wassert(extend(2000, 2015) == interval(2000, 2015));
    wassert(extend(1995, 2005) == interval(1995, 2010));
    wassert(extend(2005, 2015) == interval(2000, 2015));
    wassert(extend(1995, 2015) == interval(1995, 2015));
});

#if 0
// Check time differences
def_test(3)
{
    unique_ptr<Time> t = Time::createDifference(Time(2007, 6, 5, 4, 3, 2), Time(2006, 5, 4, 3, 2, 1));
    wasssert(actual(t) == Time(1, 1, 1, 1, 1, 1));

    t = Time::createDifference(Time(2007, 3, 1, 0, 0, 0), Time(2007, 2, 28, 0, 0, 0));
    wassert(actual(t) == Time(0, 0, 1, 0, 0, 0));

    t = Time::createDifference(create(2008, 3, 1, 0, 0, 0), create(2008, 2, 28, 0, 0, 0));
    wassert(actual(t) == Time(0, 0, 2, 0, 0, 0));

    t = Time::createDifference(create(2008, 3, 1, 0, 0, 0), create(2007, 12, 20, 0, 0, 0));
    wassert(actual(t) == Time(0, 2, 10, 0, 0, 0));

    t = Time::createDifference(create(2007, 2, 28, 0, 0, 0), create(2008, 3, 1, 0, 0, 0));
    wassert(actual(t) == Time(0, 0, 0, 0, 0, 0));
}
#endif

}

}
