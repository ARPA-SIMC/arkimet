#include "arki/core/tests.h"
#include "arki/tests/lua.h"
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_core_time");

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
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), topen.get(), t2010.get())).istrue());
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

// Test Lua functions
add_method("lua", [] {
    Time o(1, 2, 3, 4, 5, 6);
    tests::Lua test(R"(
        function test(o)
          if o.year ~= 1 then return 'o.year is '..o.year..' instead of 1' end
          if o.month ~= 2 then return 'o.month is '..o.month..' instead of 2' end
          if o.day ~= 3 then return 'o.day is '..o.day..' instead of 3' end
          if o.hour ~= 4 then return 'o.hour is '..o.hour..' instead of 4' end
          if o.minute ~= 5 then return 'o.minute is '..o.minute..' instead of 5' end
          if o.second ~= 6 then return 'o.second is '..o.second..' instead of 6' end
          if tostring(o) ~= '0001-02-03T04:05:06Z' then return 'tostring gave '..tostring(o)..' instead of 0001-02-03T04:05:06Z' end
          o1 = arki_time.time(1, 2, 3, 4, 5, 6)
          if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end
          o1 = arki_time.iso8601('1-2-3T4:5:6Z')
          if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end
          o1 = arki_time.sql('1-2-3 4:5:6')
          if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end
          o2 = arki_time.now()
          if o2 <= o1 then return 'time now is '..tostring(o2)..' which is not later than '..tostring(o1) end
        end
    )");
    test.pusharg(o);
    wassert(actual(test.run()) == "");
});

}

}
