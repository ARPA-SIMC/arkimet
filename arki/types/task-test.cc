#include "tests.h"
#include "task.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Task>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_task");

void Tests::register_tests() {

add_generic_test("task",
    { "pask" },
    "task",
    { "zask" });

}

}
