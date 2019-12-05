#include "bbox.h"
#include "arki/types/tests.h"
#include <cmath>
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_bbox");

void Tests::register_tests() {

// Experimental UTM areas
add_method("empty", [] {
});

}

}
