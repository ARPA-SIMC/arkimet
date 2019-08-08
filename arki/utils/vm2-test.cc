#include "arki/tests/tests.h"
#include <meteo-vm2/source.h>
#include "vm2.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_vm2");

void Tests::register_tests() {

// Filter stations by attributes
add_method("find_stations", [] {
  ValueBag vb1 = ValueBag::parse("rep=NONONO");
  wassert(actual(utils::vm2::find_stations(vb1).size()) == 0u);

  ValueBag vb2 = ValueBag::parse("rep=locali");
  wassert(actual(utils::vm2::find_stations(vb2).size()) > 0u);
});

// Filter variables by attributes
add_method("find_variables", [] {
  ValueBag vb1 = ValueBag::parse("bcode=NONONO");
  wassert(actual(utils::vm2::find_variables(vb1).size()) == 0u);

  ValueBag vb2 = ValueBag::parse("bcode=B13011");
  wassert(actual(utils::vm2::find_variables(vb2).size()) > 0u);
});

// Get station attributes
add_method("get_station", [] {
  ValueBag vb1 = ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
  wassert(actual(utils::vm2::get_station(1)) == vb1);

  ValueBag vb2 = ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");
  wassert(actual(utils::vm2::get_station(1)) != vb2);

  ValueBag vb3 = ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
  meteo::vm2::Source s1 = meteo::vm2::Source("inbound/meteo-vm2-source.1.lua");
  wassert(actual(utils::vm2::get_station(1, &s1)) == vb3);
  meteo::vm2::Source s2 = meteo::vm2::Source("inbound/meteo-vm2-source.2.lua");
  wassert(actual(utils::vm2::get_station(1, &s2)) != vb3);
});

// Get variable attributes
add_method("get_variable", [] {
  ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
  wassert(actual(utils::vm2::get_variable(1)) == vb1);

  ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
  wassert(actual(utils::vm2::get_variable(1)) != vb2);
});

}

}
