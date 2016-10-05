#include "arki/core/tests.h"
#include "fuzzytime.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_core_fuzzytime");

void Tests::register_tests() {

//add_method("now", [] {
//
//});

}

}
