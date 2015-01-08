/*
 * Copyright (C) 2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "types/tests.h"
#include "data.h"
#include "types/source/blob.h"
#include <wibble/sys/process.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_data_shar {
    arki_data_shar()
    {
    }
};
TESTGRP(arki_data);

namespace {

string buf_start(const wibble::sys::Buffer& buf, size_t size=4)
{
    return string((const char*)buf.data(), size);
}

}

template<> template<>
void to::test<1>()
{
    // Get the current Data instance
    Data& data = Data::current();

    auto_ptr<source::Blob> source1 = source::Blob::create("grib1", sys::process::getcwd(), "inbound/test.grib1", 0, 7218);
    auto_ptr<source::Blob> source2 = source::Blob::create("grib1", sys::process::getcwd(), "inbound/test.grib1", 7218, 34960);

    // Read two sources
    sys::Buffer buf1 = data.read(*source1);
    wassert(actual(buf1.size()) == 7218);
    wassert(actual(buf_start(buf1)) == "GRIB");

    sys::Buffer buf2 = data.read(*source2);
    wassert(actual(buf2.size()) == 34960);
    wassert(actual(buf_start(buf2)) == "GRIB");

    // Add overrides the data that are currently cached
    data.add(*source1, buf2);
    buf1 = data.read(*source1);
    wassert(actual(buf1.size()) == 34960);
    wassert(actual(buf_start(buf1)) == "GRIB");

    // Drop drops the cache for a source and causes a later reload
    data.drop(*source1);
    buf1 = data.read(*source1);
    wassert(actual(buf1.size()) == 7218);
    wassert(actual(buf_start(buf1)) == "GRIB");
}

}

