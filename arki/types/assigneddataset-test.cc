#include "tests.h"
#include "assigneddataset.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::AssignedDataset>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_assigneddataset");

void Tests::register_tests() {

// Check AssignedDataset, created with current time
add_generic_test("assigneddataset",
        { "pestname as testid imported on 2015-01-03T00:00:00", "testname as pestid imported on 2015-01-03T00:00:00", },
        {
            "testname as testid imported on 2015-01-03T00:00:00",
            // Comparison should not care about the attribution time
            "testname as testid imported on 2015-01-02T00:00:00",
            "testname as testid imported on 2015-01-04T00:00:00",
        },
        {
            "zestname as testid imported on 2015-01-03T00:00:00",
            "testname as zestid imported on 2015-01-03T00:00:00",
        });

add_lua_test("lua", "testname as testid imported on 2007-06-05T04:03:02", R"(
    function test(o)
      if o.name ~= 'testname' then return 'name is '..o.name..' instead of testname' end
      if o.id ~= 'testid' then return 'id is '..o.id..' instead of testid' end
      t = o.changed
      if t.year ~= 2007 then return 't.year is '..t.year..' instead of 2007' end
      if t.month ~= 6 then return 't.month is '..t.month..' instead of 6' end
      if t.day ~= 5 then return 't.day is '..t.day..' instead of 5' end
      if t.hour ~= 4 then return 't.hour is '..t.hour..' instead of 4' end
      if t.minute ~= 3 then return 't.minute is '..t.minute..' instead of 3' end
      if t.second ~= 2 then return 't.second is '..t.second..' instead of 2' end
      if tostring(o) ~= 'testname as testid imported on 2007-06-05T04:03:02Z' then return 'tostring gave '..tostring(o)..' instead of \'testname as testid imported on 2007-06-05T04:03:02Z\'' end
    end
)");

}

}
