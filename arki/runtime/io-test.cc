#include "arki/tests/tests.h"
#include "io.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::runtime;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_runtime_io");

void Tests::register_tests() {

add_method("tempfile", [] {
    string name;
    {
        Tempfile foo;
        name = foo.name();
        wassert(actual_file(name).exists());
    }
    wassert(actual_file(name).not_exists());
});

}

}
