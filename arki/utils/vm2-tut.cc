/*
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>
 */

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/utils/vm2.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;

struct arki_utils_vm2_shar {
};
TESTGRP(arki_utils_vm2);

// Metadata value from VM2 value
template<> template<>
void to::test<1>()
{
    ::vm2::Value value;
    value.reftime = "2012-01-01T00:00:00Z";
    value.station_id = 1;
    value.variable_id = 2;
    value.value1 = 12.0;
    value.value2 = ::vm2::MISSING_DOUBLE;
    value.value3 = "";
    value.flags = "000000000";

    Item<types::Value> mdvalue = utils::vm2::decodeMdValue(value);
    ensure_equals(mdvalue->buffer, "12,,,000000000");
}
// Filter stations by attributes
template<> template<>
void to::test<2>()
{
  ValueBag vb1 = ValueBag::parse("rep=NONONO");
  ensure(utils::vm2::Source::get().find_stations(vb1).size() == 0);

  ValueBag vb2 = ValueBag::parse("rep=locali");
  ensure(utils::vm2::Source::get().find_stations(vb2).size() > 0);
}
// Filter variables by attributes
template<> template<>
void to::test<3>()
{
  ValueBag vb1 = ValueBag::parse("bcode=NONONO");
  ensure(utils::vm2::Source::get().find_variables(vb1).size() == 0);

  ValueBag vb2 = ValueBag::parse("bcode=B13011");
  ensure(utils::vm2::Source::get().find_variables(vb2).size() > 0);
}
// Get station attributes
template<> template<>
void to::test<4>()
{
  ValueBag vb1 = ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
  ensure(utils::vm2::Source::get().get_station(1) == vb1);

  ValueBag vb2 = ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");
  ensure(utils::vm2::Source::get().get_station(1) != vb2);
}
// Get variable attributes
template<> template<>
void to::test<5>()
{
  ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
  ensure(utils::vm2::Source::get().get_variable(1) == vb1);

  ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
  ensure(utils::vm2::Source::get().get_variable(1) != vb2);
}


}
