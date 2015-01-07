/*
 * Copyright (C) 2014--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/tests.h>
#include <arki/scan/nc.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_nc_shar {
};
TESTGRP(arki_scan_nc);

// Scan a well-known nc sample
template<> template<>
void to::test<1>()
{
    Metadata md;
    scan::NetCDF scanner;
    types::Time reftime;
    wibble::sys::Buffer buf;

    scanner.open("inbound/example_1.nc");
    // See how we scan the first vm2
    wassert(actual(scanner.next(md)).istrue());

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test.vm2", 0, 34));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(227)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:00Z"));

    // FIXME: work in progress
}

}

// vim:set ts=4 sw=4:
