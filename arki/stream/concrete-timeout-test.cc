#include "arki/tests/tests.h"
#include "base.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_stream_concrete_timeout");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
