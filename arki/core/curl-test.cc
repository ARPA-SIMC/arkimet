#include "arki/tests/tests.h"
#include "arki/core/curl.h"

using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_core_curl");

void Tests::register_tests() {

add_method("empty", []() {
});

}

}
