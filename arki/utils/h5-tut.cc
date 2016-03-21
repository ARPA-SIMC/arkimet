/*
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "h5.h"
#include <arki/tests/tests.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::utils::h5;

struct arki_utils_h5_shar {
};
TESTGRP(arki_utils_h5);

template<> template<>
void to::test<1>()
{
    static const char* fname = "inbound/odimh5/IMAGE_CAPPI_v20.h5";
    MuteErrors h5e;
    File f(H5Fopen(fname, H5F_ACC_RDONLY, H5P_DEFAULT));
    wassert(actual((hid_t)f) >= 0);

    Group group(H5Gopen(f, "/dataset1/data1/quality1/how", H5P_DEFAULT));
    wassert(actual((hid_t)group) >= 0);

    Attr attr(H5Aopen(group, "task", H5P_DEFAULT));
    wassert(actual((hid_t)attr) >= 0);

    string val;
    wassert(actual(attr.read_string(val)).istrue());
    wassert(actual(val) == "Anna Fornasiero");
}

}
