#include "config.h"
#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/core/cfg.h"
#include "arki/utils/lua.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/runtime/config.h"
#include <fcntl.h>

using namespace std;
using namespace arki::tests;
using namespace arki::core;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    using arki::utils::tests::Fixture::Fixture;

    Metadata md;

    void test_setup()
    {
        md.clear();
        arki::tests::fill(md);
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_matcher");


void Tests::register_tests() {

// Test toString()
// Kind of pointless now, since it just returns the original unparsed string
add_method("tostring", [](Fixture& f) {
    wassert(actual(Matcher::parse("origin:GRIB1,1,,3 or BUFR,1").toString()) == "origin:GRIB1,1,,3 or BUFR,1");
    wassert(actual(Matcher::parse("reftime:>2015-06-01 09:00:00 % 24h").toStringExpanded()) == "reftime:>2015-06-01 09:00:00 % 24");
});

// Try OR matches
add_method("or", [](Fixture& f) {
    Matcher m;

    m = Matcher::parse("origin:GRIB1 OR BUFR");
    ensure(m(f.md));

    m = Matcher::parse("origin:BUFR or GRIB1");
    ensure(m(f.md));

    m = Matcher::parse("origin:BUFR or BUFR");
    ensure(!m(f.md));
});

// Try matching an unset metadata (see #166)
add_method("unset", [](Fixture& f) {
    Metadata md;
    Matcher m;

    m = Matcher::parse("origin:GRIB1");
    wassert_false(m(md));

    m = Matcher::parse("product:BUFR");
    wassert_false(m(md));

    m = Matcher::parse("level:GRIB1");
    wassert_false(m(md));

    m = Matcher::parse("timerange:GRIB1");
    wassert_false(m(md));

    m = Matcher::parse("proddef:GRIB");
    wassert_false(m(md));

    m = Matcher::parse("area:GRIB:foo=5");
    wassert_false(m(md));

    m = Matcher::parse("quantity:VRAD");
    wassert_false(m(md));

    m = Matcher::parse("task:test");
    wassert_false(m(md));

    m = Matcher::parse("reftime:=2018");
    wassert_false(m(md));
});

// Try using aliases
add_method("aliases", [](Fixture& f) {
    // Configuration file with alias definitions
    string test =
        "[origin]\n"
        "valid = GRIB1,1,2,3\n"
        "invalid = GRIB1,2\n"
        "[product]\n"
        "valid = GRIB1,1,2,3\n"
        "invalid = GRIB1,2\n"
        "[level]\n"
        "valid = GRIB1,110,12,13\n"
        "invalid = GRIB1,12\n"
        "[timerange]\n"
        "valid = GRIB1,2,22s,23s\n"
        "invalid = GRIB1,22\n"
        "[reftime]\n"
        "valid = >=2007,<=2008\n"
        "invalid = <2007\n";
    auto conf = core::cfg::Sections::parse(test, "memory");

    MatcherAliasDatabaseOverride aliases(conf);

    wassert(actual_matcher("origin:valid").matches(f.md));
    wassert(actual_matcher("origin:invalid").not_matches(f.md));

    wassert(actual_matcher("product:valid").matches(f.md));
    wassert(actual_matcher("product:invalid").not_matches(f.md));

    wassert(actual_matcher("level:valid").matches(f.md));
    wassert(actual_matcher("level:invalid").not_matches(f.md));

    wassert(actual_matcher("timerange:valid").matches(f.md));
    wassert(actual_matcher("timerange:invalid").not_matches(f.md));

    wassert(actual_matcher("reftime:valid").matches(f.md));
    wassert(actual_matcher("reftime:invalid").not_matches(f.md));
});

// Aliases that refer to aliases
add_method("aliases_multilevel", [](Fixture& f) {
    // Configuration file with alias definitions
    string test =
        "[origin]\n"
        "c = GRIB1,2,3,4\n"
        "b = GRIB1,1,2,3\n"
        "a = c or b\n";
    auto conf = core::cfg::Sections::parse(test, "memory");

    MatcherAliasDatabaseOverride aliases(conf);

    wassert(actual_matcher("origin:a").matches(f.md));
    wassert(actual_matcher("origin:b").matches(f.md));
    wassert(actual_matcher("origin:c").not_matches(f.md));
});

// Recursive aliases should fail
add_method("aliases_recursive_1", [](Fixture& f) {
    string test =
        "[origin]\n"
        "a = a or a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, MatcherAliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("aliases_recursive_2", [](Fixture& f) {
    string test =
        "[origin]\n"
        "a = b\n"
        "b = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, MatcherAliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("aliases_recursive_3", [](Fixture& f) {
    string test =
        "[origin]\n"
        "a = b\n"
        "b = c\n"
        "c = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, MatcherAliasDatabase db(conf));
});

// Load a file with aliases referring to other aliases
add_method("aliases_multilevel_load", [](Fixture& f) {
    File in("misc/rec-ts-alias.conf", O_RDONLY);
    auto conf = core::cfg::Sections::parse(in);
    MatcherAliasDatabaseOverride aliases(conf);
    Matcher m = Matcher::parse("timerange:f_3");
});

// Run matcher/*.txt files, doctest style
add_method("aliases_doctest", [](Fixture& f) {
    arki::Lua L;

	// Define 'ensure' function
	string lua_ensure = "function ensure_matches(val, expr)\n"
			"  local matcher = arki.matcher.new(expr)\n"
			"  if (not matcher:match(val)) then error(expr .. ' did not match ' .. tostring(val) .. ':\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_not_matches(val, expr)\n"
			"  local matcher = arki.matcher.new(expr)\n"
			"  if (matcher:match(val)) then error(expr .. ' should not have matched ' .. tostring(val) .. ':\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_matchers_equal(expr1, expr2)\n"
			"  local matcher1 = arki.matcher.new(expr1)\n"
			"  local matcher2 = arki.matcher.new(expr2)\n"
			"  if matcher1:expanded() ~= matcher2:expanded() then error(tostring(matcher1) .. '(' .. matcher1:expanded() .. ') should be the same as ' .. tostring(matcher2) .. '(' .. matcher2:expanded() .. '):\\n' .. debug.traceback()) end\n"
			"end\n";
	if (luaL_dostring(L, lua_ensure.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
                wassert(actual(error) == "");
        }

    // Run the various lua examples
    string path = "matcher";
    sys::Path dir(path);
    for (sys::Path::iterator d = dir.begin(); d != dir.end(); ++d)
    {
        if (!str::endswith(d->d_name, ".txt")) continue;
        string fname = str::joinpath(path, d->d_name);
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            wassert(actual(error) == "");
        }
    }
});

add_method("aliases_regression", [](Fixture& f) {
    Matcher m1, m2;

    m1 = Matcher::parse("origin:GRIB1 OR BUFR\n    ");
    m2 = Matcher::parse("origin:GRIB1 OR BUFR;\n   \n;   \n  ;\n");
    ensure(m1.toString() == m2.toString());
});

}

}
