#include "tests.h"
#include "reftime.h"
#include "arki/matcher.h"
#include "arki/utils/string.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using arki::core::Time;

class Tests : public TypeTestCase<types::Reftime>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_reftime");


void Tests::register_tests() {

add_generic_test(
        "position",
        { "2014-01-01T00:00:00" },
        "2015-01-02T03:04:05Z",
        // Period sorts later than Position
        { "2015-01-03T00:00:00", "2014-01-01T00:00:00 to 2014-01-31T00:00:00" },
        "=2015-01-02T03:04:05Z");

add_generic_test(
        "period",
        { "2015-01-01T00:00:00", "2015-01-10T00:00:00", "2015-01-01T00:00:00 to 2015-01-03T12:00:00" },
        "2015-01-02T00:00:00Z to 2015-01-03T00:00:00Z",
        { "2015-01-02T12:00:00 to 2015-01-31T00:00:00" });
//#warning This does not look like a query to match a period
//    t.exact_query = "=2007-06-05T04:03:02Z";

add_method("position_details", [] {
    using namespace arki::types;
    unique_ptr<Reftime> o = Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2));
    wassert(actual(o).is_reftime_position(Time(2007, 6, 5, 4, 3, 2)));

    wassert(actual(o) == Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2)));
    wassert(actual(o) != Reftime::createPosition(Time(2007, 6, 5, 4, 3, 1)));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

add_method("range", [] {
    // Check range expansion
    using namespace arki::types;
    core::Interval interval;
    Time t1(2007, 6, 5, 4, 3, 2);
    Time t1e(2007, 6, 5, 4, 3, 3);
    Time t2(2008, 7, 6, 5, 4, 3);
    Time t2e(2008, 7, 6, 5, 4, 4);
    Time t3(2007, 7, 6, 5, 4, 3);
    Time t4(2009, 8, 7, 6, 5, 4);
    Time t4e(2009, 8, 7, 6, 5, 5);
    Time t5(2009, 7, 6, 5, 4, 3);
    Time t6(2010, 9, 8, 7, 6, 5);

    // Merge with position
    Reftime::createPosition(t1)->expand_date_range(interval);
    wassert(actual(interval.begin) == t1);
    wassert(actual(interval.end) == t1e);

    // Merge with a second position
    Reftime::createPosition(t2)->expand_date_range(interval);
    wassert(actual(interval.begin) == t1);
    wassert(actual(interval.end) == t2e);

#if 0
    // Merge with a period
    Reftime::createPeriod(t3, t4)->expand_date_range(interval);
    wassert(actual(interval.begin) == t1);
    wassert(actual(interval.end) == t4e);
#endif
});

// Reproduce bugs
add_method("regression1", [] {
    auto decoded = parse("2005-12-01T18:00:00Z");
    stringstream ss;
    ss << *decoded;
    wassert(actual(ss.str()) == "2005-12-01T18:00:00Z");
});

}

}
