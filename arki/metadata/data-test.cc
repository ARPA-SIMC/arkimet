#include "arki/metadata/tests.h"
#include "data.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_data");

void Tests::register_tests() {

// Test sources
add_method("todo", []() noexcept {
});

}

}
