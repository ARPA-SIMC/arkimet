#include "encoded.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_types_encoded");

void Tests::register_tests()
{

    add_method("empty", []() noexcept {});
}

} // namespace
