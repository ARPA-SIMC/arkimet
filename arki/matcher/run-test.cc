#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/run.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_run");

void Tests::register_tests() {

// Try matching Minute run
add_method("minute", [] {
    Metadata md;
    arki::tests::fill(md);

    wassert(actual_matcher("run:MINUTE").matches(md));
    wassert(actual_matcher("run:MINUTE,12").matches(md));
    wassert(actual_matcher("run:MINUTE,12:00").matches(md));
    wassert(actual_matcher("run:MINUTE,13").not_matches(md));
    wassert(actual_matcher("run:MINUTE,12:01").not_matches(md));

    // Set a different minute
    md.test_set(Run::createMinute(9, 30));
    wassert(actual_matcher("run:MINUTE").matches(md));
    wassert(actual_matcher("run:MINUTE,09:30").matches(md));
    wassert(actual_matcher("run:MINUTE,09").not_matches(md));
    wassert(actual_matcher("run:MINUTE,09:31").not_matches(md));
});

}

}
