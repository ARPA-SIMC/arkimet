#include "fd.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_fd");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
