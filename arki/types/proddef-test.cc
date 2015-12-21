#include "tests.h"
#include "proddef.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

class Tests : public TypeTestCase<types::Proddef>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_proddef");

void Tests::register_tests() {

add_generic_test("grib",
    {},
    "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
    { "GRIB(dieci=10,undici=11,dodici=-12)" },
    "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

add_generic_test("grib_1",
    {},
    "GRIB(count=1,pippo=pippo)",
    { "GRIB(count=2,pippo=pippo)" });

add_lua_test("lua", "GRIB(uno=1,pippo=pippo)", R"(
    function test(o)
      if o.style ~= 'GRIB' then return 'style is '..o.style..' instead of GRIB' end
      v = o.val
      if v['uno'] ~= 1 then return 'v[\'uno\'] is '..v['uno']..' instead of 1' end
      if v['pippo'] ~= 'pippo' then return 'v[\'pippo\'] is '..v['pippo']..' instead of \'pippo\'' end
      if tostring(o) ~= 'GRIB(pippo=pippo, uno=1)' then return 'tostring gave '..tostring(o)..' instead of GRIB(pippo=pippo, uno=1)' end
      o1 = arki_proddef.grib{uno=1, pippo='pippo'}
      if o ~= o1 then return 'new proddef is '..tostring(o1)..' instead of '..tostring(o) end
    end
)");

}

}
