#include "tests.h"
#include "value.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Value>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_value");

void Tests::register_tests() {

// Check text value
add_generic_test("text",
    { "cia" },
    "ciao",
    { "ciap" });

// Check binary value
add_generic_test("binary",
    { "ciao" },
    "ciao♥",
    { "cia♥" });

// Check binary value with zeros
add_generic_test("zeroes",
    { "ci" },
    string("ci\0ao", 5),
    { "cia♥" });

}

}
