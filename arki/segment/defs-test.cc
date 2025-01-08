#include "arki/core/tests.h"
#include "defs.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_defs");

void Tests::register_tests() {

}

}

