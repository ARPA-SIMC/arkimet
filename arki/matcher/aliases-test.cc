#include "config.h"
#include "arki/matcher/tests.h"
#include "aliases.h"
#include "arki/metadata.h"
#include "arki/core/cfg.h"
#include "arki/utils/lua.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>

using namespace std;
using namespace arki::tests;
using namespace arki::core;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} tests("arki_matcher_aliases");


void Tests::register_tests() {

// Try using aliases
add_method("use", [] {
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

    Metadata md;
    arki::tests::fill(md);

    matcher::AliasDatabaseOverride aliases(conf);

    wassert(actual_matcher("origin:valid").matches(md));
    wassert(actual_matcher("origin:invalid").not_matches(md));

    wassert(actual_matcher("product:valid").matches(md));
    wassert(actual_matcher("product:invalid").not_matches(md));

    wassert(actual_matcher("level:valid").matches(md));
    wassert(actual_matcher("level:invalid").not_matches(md));

    wassert(actual_matcher("timerange:valid").matches(md));
    wassert(actual_matcher("timerange:invalid").not_matches(md));

    wassert(actual_matcher("reftime:valid").matches(md));
    wassert(actual_matcher("reftime:invalid").not_matches(md));
});

// Aliases that refer to aliases
add_method("multilevel", [] {
    // Configuration file with alias definitions
    string test =
        "[origin]\n"
        "c = GRIB1,2,3,4\n"
        "b = GRIB1,1,2,3\n"
        "a = c or b\n";
    auto conf = core::cfg::Sections::parse(test, "memory");

    Metadata md;
    arki::tests::fill(md);

    matcher::AliasDatabaseOverride aliases(conf);

    wassert(actual_matcher("origin:a").matches(md));
    wassert(actual_matcher("origin:b").matches(md));
    wassert(actual_matcher("origin:c").not_matches(md));
});

// Recursive aliases should fail
add_method("recursive_1", [] {
    string test =
        "[origin]\n"
        "a = a or a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("recursive_2", [] {
    string test =
        "[origin]\n"
        "a = b\n"
        "b = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("recursive_3", [] {
    string test =
        "[origin]\n"
        "a = b\n"
        "b = c\n"
        "c = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Load a file with aliases referring to other aliases
add_method("multilevel_load", [] {
    File in("misc/rec-ts-alias.conf", O_RDONLY);
    auto conf = core::cfg::Sections::parse(in);
    matcher::AliasDatabaseOverride aliases(conf);
    Matcher m = Matcher::parse("timerange:f_3");
});

// Run matcher/*.txt files, doctest style
add_method("doctest", [] {
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

add_method("regression", [] {
    Matcher m1, m2;

    m1 = Matcher::parse("origin:GRIB1 OR BUFR\n    ");
    m2 = Matcher::parse("origin:GRIB1 OR BUFR;\n   \n;   \n  ;\n");
    ensure(m1.toString() == m2.toString());
});

}

}

