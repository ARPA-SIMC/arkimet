/*
 * Copyright (C) 2009--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/metadata/tests.h>
#include <arki/scan/any.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/run.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/utils/compress.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_scan_any_shar {
};
TESTGRP(arki_scan_any);

// Scan a well-known grib file, with no padding between messages
template<> template<>
void to::test<1>()
{
	metadata::Collection mdc;
#ifndef HAVE_GRIBAPI
	ensure(not scan::scan("inbound/test.grib1", mdc));
#else
	wibble::sys::Buffer buf;

	ensure(scan::scan("inbound/test.grib1", mdc));

	ensure_equals(mdc.size(), 3u);

    // Check the source info
    wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("."), "inbound/test.grib1", 0, 7218));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	// Check origin
	ensure(mdc[0].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[0].get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(200, 0, 101)));

	// Check product
	ensure(mdc[0].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[0].get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(200, 140, 229)));

	// Check level
	ensure(mdc[0].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[0].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(1, 0)));

	// Check timerange
	ensure(mdc[0].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[0].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

    // Check area
    wassert(actual(mdc[0]).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));

    // Check proddef
    wassert(actual(mdc[0]).contains("proddef", "GRIB(tod=1)"));

	// Check reftime
	ensure_equals(mdc[0].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[0].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 8, 13, 0, 0))));

    // Check run
    wassert(actual(mdc[0]).contains("run", "MINUTE(13:00)"));


    // Check the source info
    wassert(actual(mdc[1].source).sourceblob_is("grib1", sys::fs::abspath("."), "inbound/test.grib1", 7218, 34960));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

	// Check origin
	ensure(mdc[1].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[1].get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(80, 255, 100)));

	// Check product
	ensure(mdc[1].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[1].get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(80, 2, 2)));

	// Check level
	ensure(mdc[1].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[1].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(102, 0)));

	// Check timerange
	ensure(mdc[1].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[1].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(1, 254, 0, 0)));

    // Check area
    wassert(actual(mdc[1]).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));

    // Check proddef
    wassert(actual(mdc[1]).contains("proddef", "GRIB(tod=1)"));

	// Check reftime
	ensure_equals(mdc[1].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[1].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 7, 0, 0, 0))));

	// Check run
	ensure_equals(mdc[1].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[1].get(types::TYPE_RUN), Item<>(run::Minute::create(0)));


    // Check the source info
    wassert(actual(mdc[2].source).sourceblob_is("grib1", sys::fs::abspath("."), "inbound/test.grib1", 42178, 2234));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

    wassert(actual(mdc[2]).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(mdc[2]).contains("product", "GRIB1(98, 128, 129)"));

	// Check level
	ensure(mdc[2].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[2].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(100, 1000)));

	// Check timerange
	ensure(mdc[2].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[2].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

    // Check area
    wassert(actual(mdc[2]).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(mdc[2]).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(mdc[2]).contains("reftime", "2007-10-09 00:00:00"));
    wassert(actual(mdc[2]).contains("run", "MINUTE(00:00)"));
#endif
}

// Scan a well-known bufr file, with no padding between BUFRs
template<> template<>
void to::test<2>()
{
	metadata::Collection mdc;
#ifndef HAVE_DBALLE
	ensure(not scan::scan("inbound/test.bufr", mdc));
#else
	wibble::sys::Buffer buf;

	ensure(scan::scan("inbound/test.bufr", mdc));

	ensure_equals(mdc.size(), 3u);

    // Check the source info
    wassert(actual(mdc[0].source).sourceblob_is("bufr", sys::fs::abspath("."), "inbound/test.bufr", -1, 194));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

	// Check origin
	ensure(mdc[0].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[0].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ValueBag vb;
	vb.set("t", Value::createString("synop"));
	ensure(mdc[0].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[0].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(mdc[0].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[0].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check run
	ensure(not mdc[0].has(types::TYPE_RUN));


    // Check the source info
    wassert(actual(mdc[1].source).sourceblob_is("bufr", sys::fs::abspath("."), "inbound/test.bufr", 194, 220));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(mdc[1].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[1].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(mdc[1].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[1].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(mdc[1].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[1].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not mdc[1].has(types::TYPE_RUN));


    // Check the source info
    wassert(actual(mdc[2].source).sourceblob_is("bufr", sys::fs::abspath("."), "inbound/test.bufr", 414, 220));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(mdc[2].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[2].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(mdc[2].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[2].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3, vb)));

	// Check reftime
	ensure_equals(mdc[2].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[2].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not mdc[2].has(types::TYPE_RUN));
#endif
}

// Test compression
template<> template<>
void to::test<3>()
{
	// Create a test file with 9 gribs inside
	system("cat inbound/test.grib1 inbound/test.grib1 inbound/test.grib1 > a.grib1");
	system("cp a.grib1 b.grib1");

	// Compress
	scan::compress("b.grib1", 5);
	sys::fs::deleteIfExists("b.grib1");

	{
		utils::compress::TempUnzip tu("b.grib1");
		metadata::Counter c;
		scan::scan("b.grib1", c);
		ensure_equals(c.count, 9u);
	}
}

// Test reading update sequence numbers
template<> template<>
void to::test<4>()
{
    {
        // Gribs don't have update sequence numbrs, and the usn parameter must
        // be left untouched
        metadata::Collection mdc;
        scan::scan("inbound/test.grib1", mdc);
        int usn = 42;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), false);
        ensure_equals(usn, 42);
    }

    {
        metadata::Collection mdc;
        scan::scan("inbound/synop-gts.bufr", mdc);
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 0);
    }

    {
        metadata::Collection mdc;
        scan::scan("inbound/synop-gts-usn2.bufr", mdc);
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 2);
    }
}

}

// vim:set ts=4 sw=4:
