/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata/tests.h>
#include <arki/scan/grib.h>
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
#include <arki/scan/any.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_grib_shar {
};
TESTGRP(arki_scan_grib);

// Scan a well-known grib file, with no padding between messages
template<> template<>
void to::test<1>()
{
	Metadata md;
	scan::Grib scanner;
	wibble::sys::Buffer buf;

	scanner.open("inbound/test.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/test.grib1", 0, 7218));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 0, 101)"));
    wassert(actual(md).contains("product", "GRIB1(200, 140, 229)"));
    wassert(actual(md).contains("level", "GRIB1(1, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(13:00)"));

	// Next grib
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/test.grib1", 7218, 34960));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 255, 100)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 2)"));
    wassert(actual(md).contains("level", "GRIB1(102, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(1)"));
    wassert(actual(md).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-07T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));


	// Last grib
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/test.grib1", 42178, 2234));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(md).contains("product", "GRIB1(98, 128, 129)"));
    wassert(actual(md).contains("level", "GRIB1(100, 1000)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-10-09T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

	// No more gribs
	ensure(not scanner.next(md));
}


// Scan a well-known grib file, with extra padding data between messages
template<> template<>
void to::test<2>()
{
	Metadata md;
	scan::Grib scanner;
	wibble::sys::Buffer buf;

	scanner.open("inbound/padded.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/padded.grib1", 100, 7218));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 0, 101)"));
    wassert(actual(md).contains("product", "GRIB1(200, 140, 229)"));
    wassert(actual(md).contains("level", "GRIB1(1, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(13:00)"));

	// Next grib
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/padded.grib1", 7418, 34960));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 255, 100)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 2)"));
    wassert(actual(md).contains("level", "GRIB1(102, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(1)"));
    wassert(actual(md).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-07T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

	// Last grib
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/padded.grib1", 42478, 2234));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(md).contains("product", "GRIB1(98, 128, 129)"));
    wassert(actual(md).contains("level", "GRIB1(100, 1000)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-10-09T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a well-known grib file, with no padding between GRIBs
template<> template<>
void to::test<3>()
{
	Metadata md;
	scan::Grib scanner(false, 
		"arki.year = 2008\n"
		"arki.month = 7\n"
		"arki.day = 30\n"
		"arki.hour = 12\n"
		"arki.minute = 30\n"
		"arki.second = 0\n"
		"arki.centre = 98\n"
		"arki.subcentre = 1\n"
		"arki.process = 2\n"
		"arki.origin = 1\n"
		"arki.table = 1\n"
		"arki.product = 1\n"
		"arki.ltype = 1\n"
		"arki.l1 = 1\n"
		"arki.l2 = 1\n"
		"arki.ptype = 1\n"
		"arki.punit = 1\n"
		"arki.p1 = 1\n"
		"arki.p2 = 1\n"
		"arki.bbox = { { 45.00, 11.00 }, { 46.00, 11.00 }, { 46.00, 12.00 }, { 47.00, 13.00 }, { 45.00, 12.00 } }"
	);
	wibble::sys::Buffer buf;

	scanner.open("inbound/test.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/test.grib1", 0, 7218));
}

// Test validation
template<> template<>
void to::test<4>()
{
	Metadata md;
	wibble::sys::Buffer buf;

	const scan::Validator& v = scan::grib::validator();

	int fd = open("inbound/test.grib1", O_RDONLY);

	v.validate(fd, 0, 7218, "inbound/test.grib1");
	v.validate(fd, 7218, 34960, "inbound/test.grib1");
	v.validate(fd, 42178, 2234, "inbound/test.grib1");

#define ensure_throws(x) do { try { x; ensure(false); } catch (wibble::exception::Generic& e) { } } while (0)

	ensure_throws(v.validate(fd, 1, 7217, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 0, 7217, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 0, 7219, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 7217, 34961, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 42178, 2235, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 44412, 0, "inbound/test.grib1"));
	ensure_throws(v.validate(fd, 44412, 10, "inbound/test.grib1"));

	close(fd);

	metadata::Collection mdc;
	scan::scan("inbound/test.grib1", mdc);
	buf = mdc[0].getData();

	v.validate(buf.data(), buf.size());
	ensure_throws(v.validate((const char*)buf.data()+1, buf.size()-1));
	ensure_throws(v.validate(buf.data(), buf.size()-1));
}

// Test scanning layers instead of levels
template<> template<>
void to::test<5>()
{
	Metadata md;
	scan::Grib scanner;
	wibble::sys::Buffer buf;

	scanner.open("inbound/layer.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/layer.grib1", 0, 30682));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 30682u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 30678, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 255, 45)"));
    wassert(actual(md).contains("product", "GRIB1(200, 2, 33)"));
    wassert(actual(md).contains("level", "GRIB1(110, 1, 2)"));
    wassert(actual(md).contains("timerange", "Timedef(0s, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=169,Nj=181,latfirst=-21125000,latlast=-9875000,lonfirst=-2937000,lonlast=7563000,latp=-32500000,lonp=10000000,rot=0,type=10)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=0)"));
    wassert(actual(md).contains("reftime", "2009-09-02T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a know item for which grib_api changed behaviour
template<> template<>
void to::test<6>()
{
	Metadata md;
	scan::Grib scanner;
	wibble::sys::Buffer buf;

	scanner.open("inbound/proselvo.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib1", sys::fs::abspath("."), "inbound/proselvo.grib1", 0, 298));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 298u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 294, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 98, 131)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 79)"));
    wassert(actual(md).contains("level", "GRIB1(1)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=231,Nj=265,latfirst=-16125000,latlast=375000,lonfirst=-5125000,lonlast=9250000,latp=-40000000,lonp=10000000,rot=0,type=10)"));
    wassert(actual(md).contains("proddef", "GRIB(ld=1,mt=9,nn=0,tod=1)"));
    wassert(actual(md).contains("reftime", "2010-08-11T12:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(12:00)"));

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a know item for which grib_api changed behaviour
template<> template<>
void to::test<7>()
{
	Metadata md;
	scan::Grib scanner;
	wibble::sys::Buffer buf;

    wrunchecked(scanner.open("inbound/cleps_pf16_HighPriority.grib2"));

    // See how we scan the first BUFR
    wassert(actual(scanner.next(md)).istrue());

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib2", sys::fs::abspath("."), "inbound/cleps_pf16_HighPriority.grib2", 0, 432));

    // Check that the source can be read properly
    wrunchecked(buf = md.getData());
    wassert(actual(buf.size()) == 432u);
    wassert(actual(string((const char*)buf.data(), 4)) == "GRIB");
    wassert(actual(string((const char*)buf.data() + 428, 4)) == "7777");

    wassert(actual(md).contains("origin", "GRIB2(250, 98, 4, 255, 131)"));
    wassert(actual(md).contains("product", "GRIB2(250, 2, 0, 0)"));
    wassert(actual(md).contains("level", "GRIB2S(1, -, -)"));
    wassert(actual(md).contains("timerange", "Timedef(0, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=511,Nj=415,latfirst=-16125000,latlast=9750000,lonfirst=344250000,lonlast=16125000,latp=-40000000,lonp=10000000,rot=0,tn=1)"));
    wassert(actual(md).contains("proddef", "GRIB(mc=ti,mt=0,pf=16,tf=16,tod=4,ty=3)"));
    wassert(actual(md).contains("reftime", "2013-10-22T00:00:00"));
    wassert(actual(md).contains("run", "MINUTE(00:00)"));

    // No more gribs
    wassert(actual(scanner.next(md)).isfalse());
}

// Scan a GRIB2 with experimental UTM areas
template<> template<>
void to::test<8>()
{
    Metadata md;
    scan::Grib scanner;
    scanner.open("inbound/calmety_20110215.grib2");
    ensure(scanner.next(md));

    wassert(actual(md).contains("origin", "GRIB2(00200, 00000, 000, 000, 203)"));
    wassert(actual(md).contains("product", "GRIB2(200, 0, 200, 33)"));
    wassert(actual(md).contains("level", "GRIB2S(103, 0, 10)"));
    wassert(actual(md).contains("timerange", "Timedef(0s, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=90, Nj=52, fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500, tn=32768, utm=1, zone=32)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=0)"));
    wassert(actual(md).contains("reftime", "2011-02-15T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

    // No more gribs
    ensure(not scanner.next(md));
}

// Check scanning of some Timedef cases
template<> template<>
void to::test<9>()
{
    {
        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/ninfa_ana.grib2");
        ensure(scanner.next(md));

        wassert(actual(md).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/ninfa_forc.grib2");
        ensure(scanner.next(md));

        wassert(actual(md).contains("timerange", "Timedef(3h,254,0s)"));
    }
}

// Check scanning COSMO nudging timeranges
template<> template<>
void to::test<10>()
{
    WIBBLE_TEST_INFO(info);

    {
        metadata::Collection mdc;
        wrunchecked(scan::scan("inbound/cosmonudging-t2.grib1", mdc));
        wassert(actual(mdc.size()) == 35u);
        for (unsigned i = 0; i < 5; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[5]).contains("timerange", "Timedef(0s, 2, 1h)"));
        wassert(actual(mdc[6]).contains("timerange", "Timedef(0s, 3, 1h)"));
        for (unsigned i = 7; i < 13; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[13]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 14; i < 19; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[19]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[20]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 21; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 27; i < 35; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s, 0, 12h)"));
    }
    {
        metadata::Collection mdc;
        wrunchecked(scan::scan("inbound/cosmonudging-t201.grib1", mdc));
        wassert(actual(mdc.size()) == 33u);
        wassert(actual(mdc[0]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[1]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[2]).contains("timerange", "Timedef(0s, 0, 12h)"));
        for (unsigned i = 3; i < 16; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[16]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[17]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 18; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 2, 1h)"));
        for (unsigned i = 27; i < 33; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        metadata::Collection mdc;
        wrunchecked(scan::scan("inbound/cosmonudging-t202.grib1", mdc));
        wassert(actual(mdc.size()) == 11u);
        for (unsigned i = 0; i < 11; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }


    // Shortcut to read one single GRIB from a file
    struct OneGrib
    {
        wibble::tests::LocationInfo& wibble_test_location_info;
        Metadata md;

        OneGrib(wibble::tests::LocationInfo& info) : wibble_test_location_info(info) {}
        void read(const char* fname)
        {
            wibble_test_location_info() << "Sample: " << fname;
            metadata::Collection mdc;
            wrunchecked(scan::scan(fname, mdc));
            wassert(actual(mdc.size()) == 1u);
            wrunchecked(md = mdc[0]);
        }
    };

    OneGrib md(info);

    md.read("inbound/cosmonudging-t203.grib1");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));

    md.read("inbound/cosmo/anist_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fc0ist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == std::string("Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/anproc_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_3.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,2,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_4.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,12h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fcist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,1,6h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_3.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(48h,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));
}

// Check scanning a GRIB2 with a bug in level scanning code
template<> template<>
void to::test<11>()
{
    // FIXME: It is unsure what is the correct expected behaviour here, across
    // different versions of grib_api
#if 0
    metadata::Collection mdc;
    scan::scan("inbound/wronglevel.grib2", mdc);
    ensure_equals(mdc.size(), 1u);
    ensure_equals(mdc[0].get<Level>(), Level::decodeString("GRIB2S(101,-,-)"));
#endif
}

// Check opening very long GRIB files for scanning
template<> template<>
void to::test<12>()
{
	scan::Grib scanner;
	int fd = open("bigfile.grib1", O_WRONLY | O_CREAT, 0644);
	ensure(fd != -1);
	ensure(ftruncate(fd, 0xFFFFFFFF) != -1);
	close(fd);
	scanner.open("bigfile.grib1");
}

}
