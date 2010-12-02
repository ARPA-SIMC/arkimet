/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/tests/test-utils.h>
#include <arki/dataset/test-scenario.h>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dataset_test_scenario_shar {
    arki_dataset_test_scenario_shar()
    {
    }
};
TESTGRP(arki_dataset_test_scenario);

// Simple generation
template<> template<>
void to::test<1>()
{
    using namespace dataset;
    /* const test::Scenario& scen = */ test::Scenario::get("ondisk2-archived");
}

template<> template<>
void to::test<2>()
{
    using namespace dataset;
    /* const test::Scenario& scen = */ test::Scenario::get("ondisk2-manyarchivestates");
}

}

// vim:set ts=4 sw=4:
