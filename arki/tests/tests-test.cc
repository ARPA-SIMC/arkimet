#include "arki/tests/tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_tests");

void Tests::register_tests() {

add_method("basic", [] {
    wassert(actual(true).istrue());
    wassert(actual(false).isfalse());
    wassert(actual(3) == 3);
    wassert(actual(3) != 4);
    wassert(actual(3) < 4);
    wassert(actual(3) <= 4);
    wassert(actual(3) <= 3);
    wassert(actual(4) > 3);
    wassert(actual(4) >= 3);
    wassert(actual(3) >= 3);
    wassert(actual("ciao").startswith("ci"));
    wassert(actual("ciao").endswith("ao"));
    wassert(actual("ciao").contains("ci"));
    wassert(actual("ciao").contains("ia"));
    wassert(actual("ciao").contains("ao"));
    wassert(actual("ciao").matches("[ia]+"));
});

}

}
