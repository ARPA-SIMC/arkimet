#include "tests.h"
#include "local.h"

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_local");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
