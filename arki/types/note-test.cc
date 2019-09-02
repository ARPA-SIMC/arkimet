#include "tests.h"
#include "note.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Note>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_note");

void Tests::register_tests() {

// Check Note, created with arbitrary time
add_generic_test("note",
    { "[2015-01-02T00:00:00]foo", "[2015-01-03T00:00:00]bar" },
    "[2015-01-03T00:00:00]foo",
    { "[2015-01-03T00:00:00]zab", "[2015-01-04T00:00:00]bar", });

}

}
