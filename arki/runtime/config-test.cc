#include "arki/tests/tests.h"
#include "arki/core/cfg.h"
#include "arki/runtime/config.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::runtime;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_runtime_config");

void Tests::register_tests() {

// Test restrict functions
add_method("empty", [] {
});

}

}
