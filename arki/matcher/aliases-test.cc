#include "config.h"
#include "arki/matcher/tests.h"
#include "arki/matcher/parser.h"
#include "aliases.h"
#include "arki/metadata.h"
#include "arki/core/cfg.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>

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
    std::string test =
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
    matcher::Parser parser;
    parser.load_aliases(conf);

    Metadata md;
    arki::tests::fill(md);

    wassert(actual_matcher(parser, "origin:valid").matches(md));
    wassert(actual_matcher(parser, "origin:invalid").not_matches(md));

    wassert(actual_matcher(parser, "product:valid").matches(md));
    wassert(actual_matcher(parser, "product:invalid").not_matches(md));

    wassert(actual_matcher(parser, "level:valid").matches(md));
    wassert(actual_matcher(parser, "level:invalid").not_matches(md));

    wassert(actual_matcher(parser, "timerange:valid").matches(md));
    wassert(actual_matcher(parser, "timerange:invalid").not_matches(md));

    wassert(actual_matcher(parser, "reftime:valid").matches(md));
    wassert(actual_matcher(parser, "reftime:invalid").not_matches(md));
});

// Aliases that refer to aliases
add_method("multilevel", [] {
    // Configuration file with alias definitions
    std::string test =
        "[origin]\n"
        "c = GRIB1,2,3,4\n"
        "b = GRIB1,1,2,3\n"
        "a = c or b\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    matcher::Parser parser;
    parser.load_aliases(conf);

    Metadata md;
    arki::tests::fill(md);

    wassert(actual_matcher(parser, "origin:a").matches(md));
    wassert(actual_matcher(parser, "origin:b").matches(md));
    wassert(actual_matcher(parser, "origin:c").not_matches(md));
});

// Recursive aliases should fail
add_method("recursive_1", [] {
    std::string test =
        "[origin]\n"
        "a = a or a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("recursive_2", [] {
    std::string test =
        "[origin]\n"
        "a = b\n"
        "b = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Recursive aliases should fail
add_method("recursive_3", [] {
    std::string test =
        "[origin]\n"
        "a = b\n"
        "b = c\n"
        "c = a\n";
    auto conf = core::cfg::Sections::parse(test, "memory");
    wassert_throws(std::runtime_error, matcher::AliasDatabase db(conf));
});

// Load a file with aliases referring to other aliases
add_method("multilevel_load", [] {
    matcher::Parser parser;
    File in("misc/rec-ts-alias.conf", O_RDONLY);
    parser.load_aliases(core::cfg::Sections::parse(in));
    Matcher m = parser.parse("timerange:f_3");
});

}

}

