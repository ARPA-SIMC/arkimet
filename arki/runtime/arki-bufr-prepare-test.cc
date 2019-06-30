#include "arki/tests/tests.h"
#include "arki/runtime/tests.h"
#include "arki-bufr-prepare.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
};

Tests test("arki_runtime_arkibufrprepare");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
