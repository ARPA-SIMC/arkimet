#include "arki/tests/tests.h"
#include "accounting.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_accounting");

void Tests::register_tests() {

// Simple counter test
add_method("counter", [] {
    utils::acct::Counter counter("foo");
    wassert(actual(counter.name()) == "foo");
    wassert(actual(counter.val()) == 0u);
    counter.incr();
    wassert(actual(counter.val()) == 1u);
    counter.incr(10);
    wassert(actual(counter.val()) == 11u);
    counter.reset();
    wassert(actual(counter.val()) == 0u);
});

}

}
