#include "arki/tests/tests.h"
#include "reporter.h"

using namespace std;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_reporter");

void Tests::register_tests() {

add_method("empty", []() noexcept {
});

}

}

