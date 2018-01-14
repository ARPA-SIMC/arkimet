#include "arki/tests/tests.h"
#include "lua.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_formatter_lua");

void Tests::register_tests() {

// null value
add_method("empty", [] {
});

}

}
