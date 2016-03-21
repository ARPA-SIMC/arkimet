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

add_lua_test("lua", "task", R"(
    function test(o)
      if o.task ~= 'task' then return 'content is '..o.task..' instead of task' end
      if tostring(o) ~= 'task' then return 'tostring gave '..tostring(o)..' instead of task' end
    end
)");

}

}
