#include "tests.h"
#include "base.h"
#include "arki/core/file.h"
#include <vector>

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

Tests test("arki_stream_discard");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
