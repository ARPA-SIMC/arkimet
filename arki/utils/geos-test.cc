#include "arki/core/tests.h"
#include "arki/utils/geos.h"
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_geos");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
