#include "arki/metadata/tests.h"
#include "validator.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_validator");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
