#include "field.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Field>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_field");

void Tests::register_tests()
{
    add_generic_test("field", {"bield"}, "field", {"yield"});
}

} // namespace
