#include "config.h"

#include <arki/tests/tests.h>
#include <arki/utils/vm2.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_utils_vm2_shar {
};
TESTGRP(arki_utils_vm2);

// Filter stations by attributes
template<> template<>
void to::test<1>()
{
  ValueBag vb1 = ValueBag::parse("rep=NONONO");
  ensure(utils::vm2::find_stations(vb1).size() == 0);

  ValueBag vb2 = ValueBag::parse("rep=locali");
  ensure(utils::vm2::find_stations(vb2).size() > 0);
}
// Filter variables by attributes
template<> template<>
void to::test<2>()
{
  ValueBag vb1 = ValueBag::parse("bcode=NONONO");
  ensure(utils::vm2::find_variables(vb1).size() == 0);

  ValueBag vb2 = ValueBag::parse("bcode=B13011");
  ensure(utils::vm2::find_variables(vb2).size() > 0);
}
// Get station attributes
template<> template<>
void to::test<3>()
{
  ValueBag vb1 = ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
  ensure(utils::vm2::get_station(1) == vb1);

  ValueBag vb2 = ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");
  ensure(utils::vm2::get_station(1) != vb2);
}
// Get variable attributes
template<> template<>
void to::test<4>()
{
  ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
  ensure(utils::vm2::get_variable(1) == vb1);

  ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
  ensure(utils::vm2::get_variable(1) != vb2);
}


}
