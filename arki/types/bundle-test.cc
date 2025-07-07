#include "bundle.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_types_bundle");

void Tests::register_tests()
{

    add_method("empty", []() noexcept {});
}

} // namespace
