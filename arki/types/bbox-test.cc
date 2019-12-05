#include <arki/types/tests.h>
#include <arki/types/bbox.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_types_bbox");

void Tests::register_tests() {

// Check INVALID
add_method("invalid", [] {
    tests::TestGenericType t("bbox", "INVALID");
    wassert(t.check());
});

}

}
