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

add_lua_test("lua", "MINUTE(12:30)", R"(
    function test(o)
      if o.style ~= 'MINUTE' then return 'style is '..o.style..' instead of MINUTE' end
      if o.hour ~= 12 then return 'o.hour is '..o.hour..' instead of 12' end
      if o.min ~= 30 then return 'o.min is '..o.min..' instead of 30' end
      if tostring(o) ~= 'MINUTE(12:30)' then return 'tostring gave '..tostring(o)..' instead of MINUTE(12:30)' end
      local o1 = arki_run.minute(12, 30)
      if o ~= o1 then return 'new run is '..tostring(o1)..' instead of '..tostring(o) end
    end
)");

}

}
