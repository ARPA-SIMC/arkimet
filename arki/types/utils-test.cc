#include "tests.h"
#include "utils.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_types_utils");

void Tests::register_tests() {

add_method("empty", []() noexcept {
});

}

}
