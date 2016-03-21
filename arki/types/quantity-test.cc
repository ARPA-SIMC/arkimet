#include "tests.h"
#include "quantity.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Quantity>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_quantity");

void Tests::register_tests() {

add_generic_test("quantity",
    { "a", "a,a", "1,b,c", "a,1,c", "a,b,1", "1,2,3", },
    "a,b,c",
    { "b", "c", "c,d", "c,d,e" });

}

}
