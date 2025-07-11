#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/matcher/reftime.h"
#include "arki/matcher/tests.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_reftime");

std::string sql(const Matcher& m, const std::string& name)
{
    auto o = m.get(TYPE_REFTIME);
    if (!o)
        throw std::runtime_error("no reftime matcher found");
    return o->toReftimeSQL(name);
}

void Tests::register_tests()
{

    // Try matching reference times
    add_method("match_position", [] {
        matcher::Parser parser;
        Metadata md;
        arki::tests::fill(md);

        // md.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3,
        // 4, 5)));

        wassert(actual_matcher("reftime:>=2007").matches(md));
        wassert(actual(sql(parser.parse("reftime:>=2007"), "foo")) ==
                "(foo>='2007-01-01 00:00:00')");
        wassert(actual_matcher("reftime:<=2007").matches(md));
        wassert(actual_matcher("reftime:>2006").matches(md));
        wassert(actual_matcher("reftime:<2008").matches(md));
        wassert(actual_matcher("reftime:==2007").matches(md));
        wassert(actual_matcher("reftime:=2007").matches(md));
        wassert(actual_matcher("reftime:>=2008").not_matches(md));
        wassert(actual_matcher("reftime:<=2006").not_matches(md));
        wassert(actual_matcher("reftime:>2007").not_matches(md));
        wassert(actual_matcher("reftime:<2007").not_matches(md));
        wassert(actual_matcher("reftime:==2006").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:==2006"), "foo")) ==
                "((foo>='2006-01-01 00:00:00' AND foo<'2007-01-01 00:00:00'))");

        wassert(actual_matcher("reftime:>=2007-01").matches(md));
        wassert(actual_matcher("reftime:<=2007-01").matches(md));
        wassert(actual_matcher("reftime:>2006-12").matches(md));
        wassert(actual_matcher("reftime:<2007-02").matches(md));
        wassert(actual_matcher("reftime:==2007-01").matches(md));
        wassert(actual_matcher("reftime:=2007-01").matches(md));
        wassert(actual_matcher("reftime:>=2007-02").not_matches(md));
        wassert(actual_matcher("reftime:<=2006-12").not_matches(md));
        wassert(actual_matcher("reftime:>2007-01").not_matches(md));
        wassert(actual_matcher("reftime:<2007-01").not_matches(md));
        wassert(actual_matcher("reftime:==2007-02").not_matches(md));

        wassert(actual_matcher("reftime:>=2007-01-02").matches(md));
        wassert(actual_matcher("reftime:<=2007-01-02").matches(md));
        wassert(actual_matcher("reftime:>2007-01-01").matches(md));
        wassert(actual_matcher("reftime:<2007-01-03").matches(md));
        wassert(actual_matcher("reftime:==2007-01-02").matches(md));
        wassert(actual_matcher("reftime:=2007-01-02").matches(md));
        wassert(actual_matcher("reftime:>=2007-01-03").not_matches(md));
        wassert(actual_matcher("reftime:<=2006-01-01").not_matches(md));
        wassert(actual_matcher("reftime:>2007-01-02").not_matches(md));
        wassert(actual_matcher("reftime:<2007-01-02").not_matches(md));
        wassert(actual_matcher("reftime:==2007-01-01").not_matches(md));

        wassert(actual_matcher("reftime:>=2007-01-02 03").matches(md));
        wassert(actual_matcher("reftime:<=2007-01-02 03").matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 02").matches(md));
        wassert(actual_matcher("reftime:<2007-01-02 04").matches(md));
        wassert(actual_matcher("reftime:==2007-01-02 03").matches(md));
        wassert(actual_matcher("reftime:=2007-01-02 03").matches(md));
        wassert(actual_matcher("reftime:>=2007-01-02 04").not_matches(md));
        wassert(actual_matcher("reftime:<=2006-01-02 02").not_matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 03").not_matches(md));
        wassert(actual_matcher("reftime:<2007-01-02 03").not_matches(md));
        wassert(actual_matcher("reftime:==2007-01-02 02").not_matches(md));

        wassert(actual_matcher("reftime:>=2007-01-02 03:04").matches(md));
        wassert(actual_matcher("reftime:<=2007-01-02 03:04").matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 03:03").matches(md));
        wassert(actual_matcher("reftime:<2007-01-03 03:05").matches(md));
        wassert(actual_matcher("reftime:==2007-01-02 03:04").matches(md));
        wassert(actual_matcher("reftime:=2007-01-02 03:04").matches(md));
        wassert(actual_matcher("reftime:>=2007-01-02 03:05").not_matches(md));
        wassert(actual_matcher("reftime:<=2006-01-02 03:03").not_matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 03:04").not_matches(md));
        wassert(actual_matcher("reftime:<2007-01-02 03:04").not_matches(md));
        wassert(actual_matcher("reftime:==2007-01-02 03:03").not_matches(md));

        wassert(actual_matcher("reftime:>=2007-01-02 03:04:05").matches(md));
        wassert(actual_matcher("reftime:<=2007-01-02 03:04:05").matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 03:04:04").matches(md));
        wassert(actual_matcher("reftime:<2007-01-03 03:04:06").matches(md));
        wassert(actual_matcher("reftime:==2007-01-02 03:04:05").matches(md));
        wassert(actual_matcher("reftime:=2007-01-02 03:04:05").matches(md));
        wassert(
            actual_matcher("reftime:>=2007-01-02 03:04:06").not_matches(md));
        wassert(
            actual_matcher("reftime:<=2006-01-02 03:04:04").not_matches(md));
        wassert(actual_matcher("reftime:>2007-01-02 03:04:05").not_matches(md));
        wassert(actual_matcher("reftime:<2007-01-02 03:04:05").not_matches(md));
        wassert(
            actual_matcher("reftime:==2007-01-02 03:04:04").not_matches(md));
    });

    // Special cases that caused bugs
    add_method("regression1", [] {
        Metadata md;

        md.test_set(Reftime::createPosition(Time(2007, 1, 1, 0, 0, 0)));
        wassert(actual_matcher("reftime:>2006").matches(md));
        // ensure_equals(Matcher::parse("reftime:>2006").toString(),
        // "reftime:>2006-12-31 23:59:59");
    });

    // Try matching times
    add_method("times", [] {
        matcher::Parser parser;
        Metadata md;
        arki::tests::fill(md);

        // 2007-01-02 03:04:05

        wassert(actual_matcher("reftime:=03").matches(md));
        wassert(actual_matcher("reftime:=02").not_matches(md));
        wassert(actual_matcher("reftime:==03").matches(md));
        wassert(actual_matcher("reftime:==02").not_matches(md));
        wassert(actual_matcher("reftime:>=03").matches(md));
        wassert(actual_matcher("reftime:>=02").matches(md));
        wassert(actual_matcher("reftime:>=04").not_matches(md));
        wassert(actual_matcher("reftime:<=03").matches(md));
        wassert(actual_matcher("reftime:<=04").matches(md));
        wassert(actual_matcher("reftime:<=02").not_matches(md));
        wassert(actual_matcher("reftime:>02").matches(md));
        wassert(actual_matcher("reftime:<04").matches(md));
        wassert(actual_matcher("reftime:>03").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:>03"), "foo")) ==
                "(TIME(foo)>'03:59:59')");
        wassert(actual_matcher("reftime:<03").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:<03"), "foo")) ==
                "(TIME(foo)<'03:00:00')");

        wassert(actual_matcher("reftime:=03:04").matches(md));
        wassert(actual_matcher("reftime:=03:03").not_matches(md));
        wassert(actual_matcher("reftime:==03:04").matches(md));
        wassert(actual_matcher("reftime:==03:03").not_matches(md));
        wassert(actual_matcher("reftime:>=03:04").matches(md));
        wassert(actual_matcher("reftime:>=03:03").matches(md));
        wassert(actual_matcher("reftime:>=03:05").not_matches(md));
        wassert(actual_matcher("reftime:<=03:04").matches(md));
        wassert(actual_matcher("reftime:<=03:05").matches(md));
        wassert(actual_matcher("reftime:<=03:03").not_matches(md));
        wassert(actual_matcher("reftime:>03:03").matches(md));
        wassert(actual_matcher("reftime:<03:05").matches(md));
        wassert(actual_matcher("reftime:>03:04").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:>03:04"), "foo")) ==
                "(TIME(foo)>'03:04:59')");
        wassert(actual_matcher("reftime:<03:04").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:<03:04"), "foo")) ==
                "(TIME(foo)<'03:04:00')");

        wassert(actual_matcher("reftime:=03:04:05").matches(md));
        wassert(actual_matcher("reftime:=03:04:04").not_matches(md));
        wassert(actual_matcher("reftime:==03:04:05").matches(md));
        wassert(actual_matcher("reftime:==03:04:04").not_matches(md));
        wassert(actual_matcher("reftime:>=03:04:05").matches(md));
        wassert(actual_matcher("reftime:>=03:04:04").matches(md));
        wassert(actual_matcher("reftime:>=03:04:06").not_matches(md));
        wassert(actual_matcher("reftime:<=03:04:05").matches(md));
        wassert(actual_matcher("reftime:<=03:04:06").matches(md));
        wassert(actual_matcher("reftime:<=03:04:04").not_matches(md));
        wassert(actual_matcher("reftime:>03:04:04").matches(md));
        wassert(actual_matcher("reftime:<03:04:06").matches(md));
        wassert(actual_matcher("reftime:>03:04:05").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:>03:04:05"), "foo")) ==
                "(TIME(foo)>'03:04:05')");
        wassert(actual_matcher("reftime:<03:04:05").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:<03:04:05"), "foo")) ==
                "(TIME(foo)<'03:04:05')");
    });

    // Try matching time repetitions
    add_method("time_repetition", [] {
        matcher::Parser parser;
        Metadata md;
        arki::tests::fill(md);

        wassert(actual_matcher("reftime:>2007-01-02 00:04:05%3h").matches(md));
        wassert(actual_matcher("reftime:<2007-01-02 06:04:05%3h").matches(md));
        wassert(
            actual_matcher("reftime:>2007-01-02 00:04:05%6h").not_matches(md));
        wassert(
            actual_matcher("reftime:>2007-01-02 03:00:00%3h").not_matches(md));
        wassert(actual(sql(parser.parse("reftime:>2007-01-02 00:04:05%6h"),
                           "foo")) ==
                "(foo>='2007-01-02 00:04:06' AND (TIME(foo)=='00:04:05' OR "
                "TIME(foo)=='06:04:05' OR TIME(foo)=='12:04:05' OR "
                "TIME(foo)=='18:04:05'))");

        wassert(actual_matcher("reftime:>=2007-01-01 00:00 %24h")
                    .matches("reftime:2007-01-01T00:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 00:00 %24h")
                    .matches("reftime:2007-01-03T00:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 00:00 %24h")
                    .not_matches("reftime:2006-12-31T00:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 00:00 %24h")
                    .not_matches("reftime:2007-01-01T12:00:00Z"));

        wassert(actual_matcher("reftime:>=2007-01-01 12:00 @12:00 every 24h")
                    .matches("reftime:2007-01-01T12:00:00Z"));

        wassert(actual_matcher("reftime:>=2007-01-01 12:00 % 24h")
                    .matches("reftime:2007-01-01T12:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 12:00 % 24h")
                    .matches("reftime:2007-01-03T12:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 12:00 % 24h")
                    .not_matches("reftime:2006-12-31T12:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 12:00 % 24h")
                    .not_matches("reftime:2007-01-01T00:00:00Z"));
        wassert(actual_matcher("reftime:>=2007-01-01 12:00 % 24h")
                    .not_matches("reftime:2007-01-02T00:00:00Z"));
    });
}

} // namespace
