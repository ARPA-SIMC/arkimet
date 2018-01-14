#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/task.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_task");

void Tests::register_tests() {

add_method("odim", [] {
    Metadata md;
    arki::tests::fill(md);

    ensure_matches("task:", md);
    ensure_matches("task:task1", md);   //match esatto
    ensure_matches("task:TASK1", md);   //match case insensitive
    ensure_matches("task:ASK", md);     //match per sottostringa

    ensure_not_matches("task:baaaaa", md);
});

}

}
