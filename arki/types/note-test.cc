#include "tests.h"
#include "note.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Note>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_note");

void Tests::register_tests() {

// Check Note, created with arbitrary time
add_generic_test("note",
    { "[2015-01-02T00:00:00]foo", "[2015-01-03T00:00:00]bar" },
    "[2015-01-03T00:00:00]foo",
    { "[2015-01-03T00:00:00]zab", "[2015-01-04T00:00:00]bar", });

add_lua_test("lua", "[2007-06-05T04:03:02]test", R"(
    function test(o)
      if o.content ~= 'test' then return 'content is '..o.content..' instead of test' end
      t = o.time
      if t.year ~= 2007 then return 't.year is '..t.year..' instead of 2007' end
      if t.month ~= 6 then return 't.month is '..t.month..' instead of 6' end
      if t.day ~= 5 then return 't.day is '..t.day..' instead of 5' end
      if t.hour ~= 4 then return 't.hour is '..t.hour..' instead of 4' end
      if t.minute ~= 3 then return 't.minute is '..t.minute..' instead of 3' end
      if t.second ~= 2 then return 't.second is '..t.second..' instead of 2' end
      if tostring(o) ~= '[2007-06-05T04:03:02Z]test' then return 'tostring gave '..tostring(o)..' instead of [2007-06-05T04:03:02Z]test' end
    end
)");

}

}
