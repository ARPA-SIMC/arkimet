#include "tests.h"
#include "run.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Run>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_run");

void Tests::register_tests() {

// Check MINUTE
add_generic_test("minute",
    { "MINUTE(00)", "MINUTE(11)", "MINUTE(11:00)", },
    vector<string>({ "MINUTE(12:00)", "MINUTE(12)" }),
    { "MINUTE(12:01)", "MINUTE(13)", },
    "MINUTE,12:00");

}

}
