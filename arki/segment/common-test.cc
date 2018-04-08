#include "tests.h"
#include "common.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} tests("arki_segment_common");


void Tests::register_tests() {

add_method("empty", []{
});

}
}
