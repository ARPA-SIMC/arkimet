/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
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
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_grib_shar {
};
TESTGRP(arki_scan_grib);

#define ensure_md_equals(md, type, strval) \
    ensure_equals((md).get<type>(), type::decodeString(strval))

// Scan a well-known grib file, with no padding between messages
template<> template<>
void to::test<1>()
{
	Metadata md;
	scan::Grib scanner;
	ValueBag vb;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/test.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 0, 7218)));

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

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(200, 0, 101)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(200, 140, 229)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(1, 0)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(97));
	vb.set("Nj", Value::createInteger(73));
	vb.set("latfirst", Value::createInteger(40000000));
	vb.set("latlast", Value::createInteger(46000000));
	vb.set("lonfirst", Value::createInteger(12000000));
	vb.set("lonlast", Value::createInteger(20000000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 8, 13, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(13)));


	// Next grib
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 7218, 34960)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

    // Check origin
    ensure_md_equals(md, Origin, "GRIB1(80, 255, 100)");

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(80, 2, 2)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(102, 0)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(1, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(205));
	vb.set("Nj", Value::createInteger(85));
	vb.set("latfirst", Value::createInteger(30000000));
	vb.set("latlast", Value::createInteger(72000000));
	vb.set("lonfirst", Value::createInteger(-60000000));
	vb.set("lonlast", Value::createInteger(42000000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 7, 0, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(0)));


	// Last grib
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 42178, 2234)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(98, 0, 129)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(98, 128, 129)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(100, 1000)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(43));
	vb.set("Nj", Value::createInteger(25));
	vb.set("latfirst", Value::createInteger(55500000));
	vb.set("latlast", Value::createInteger(31500000));
	vb.set("lonfirst", Value::createInteger(-11500000));
	vb.set("lonlast", Value::createInteger(30500000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 10, 9, 0, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(0)));

	// No more gribs
	ensure(not scanner.next(md));
}


// Scan a well-known grib file, with extra padding data between messages
template<> template<>
void to::test<2>()
{
	Metadata md;
	scan::Grib scanner;
	ValueBag vb;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/padded.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/padded.grib1"), 100, 7218)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(200, 0, 101)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(200, 140, 229)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(1, 0)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(97));
	vb.set("Nj", Value::createInteger(73));
	vb.set("latfirst", Value::createInteger(40000000));
	vb.set("latlast", Value::createInteger(46000000));
	vb.set("lonfirst", Value::createInteger(12000000));
	vb.set("lonlast", Value::createInteger(20000000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 8, 13, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(13)));


	// Next grib
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/padded.grib1"), 7418, 34960)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(80, 255, 100)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(80, 2, 2)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(102, 0)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(1, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(205));
	vb.set("Nj", Value::createInteger(85));
	vb.set("latfirst", Value::createInteger(30000000));
	vb.set("latlast", Value::createInteger(72000000));
	vb.set("lonfirst", Value::createInteger(-60000000));
	vb.set("lonlast", Value::createInteger(42000000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 7, 0, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(0)));


	// Last grib
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/padded.grib1"), 42478, 2234)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(98, 0, 129)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(98, 128, 129)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(100, 1000)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(43));
	vb.set("Nj", Value::createInteger(25));
	vb.set("latfirst", Value::createInteger(55500000));
	vb.set("latlast", Value::createInteger(31500000));
	vb.set("lonfirst", Value::createInteger(-11500000));
	vb.set("lonlast", Value::createInteger(30500000));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 10, 9, 0, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(0)));


	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a well-known grib file, with no padding between BUFRs
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
	ValueBag vb;
	wibble::sys::Buffer buf;

	scanner.open("inbound/test.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 0, 7218)));
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
	ValueBag vb;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/layer.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/layer.grib1"), 0, 30682)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 30682u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 30678, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(200, 255, 45)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(200, 2, 33)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(110, 1, 2)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
    ensure_md_equals(md, Timerange, "Timedef(0s, 254)");

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(169));
	vb.set("Nj", Value::createInteger(181));
	vb.set("latfirst", Value::createInteger(-21125000));
	vb.set("latlast", Value::createInteger(-9875000));
	vb.set("lonfirst", Value::createInteger(-2937000));
	vb.set("lonlast", Value::createInteger(7563000));
	vb.set("latp", Value::createInteger(-32500000));
	vb.set("lonp", Value::createInteger(10000000));
	vb.set("rot", Value::createInteger(0));
	vb.set("type", Value::createInteger(10));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    vb.clear();
    ensure_md_equals(md, Proddef, "GRIB(tod=0)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2009, 9, 2, 0, 0, 0))));

    // Check run
    ensure_md_equals(md, Run, "MINUTE(00:00)");

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a know item for which grib_api changed behaviour
template<> template<>
void to::test<6>()
{
	Metadata md;
	scan::Grib scanner;
	ValueBag vb;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/proselvo.grib1");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/proselvo.grib1"), 0, 298)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 298u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 294, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(80, 98, 131)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(80, 2, 79)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(1)));

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(231));
	vb.set("Nj", Value::createInteger(265));
	vb.set("latfirst", Value::createInteger(-16125000));
	vb.set("latlast", Value::createInteger(375000));
	vb.set("lonfirst", Value::createInteger(-5125000));
	vb.set("lonlast", Value::createInteger(9250000));
	vb.set("latp", Value::createInteger(-40000000));
	vb.set("lonp", Value::createInteger(10000000));
	vb.set("rot", Value::createInteger(0));
	vb.set("type", Value::createInteger(10));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(ld=1,mt=9,nn=0,tod=1)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2010, 8, 11, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a know item for which grib_api changed behaviour
template<> template<>
void to::test<7>()
{
	Metadata md;
	scan::Grib scanner;
	ValueBag vb;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/cleps_grib2.grib2");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("grib2", sys::fs::abspath("inbound/cleps_grib2.grib2"), 0, 415)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 415u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 411, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::GRIB2::create(250, 98, 4, 255, 131)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::GRIB2::create(250, 0, 2, 22)));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(level::GRIB2S::create(103, 0, 10)));

    // Check timerange
    ensure(md.get(types::TYPE_TIMERANGE).defined());
    ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::Timedef::create(
                    0, timerange::Timedef::UNIT_SECOND,
                    2, 3, timerange::Timedef::UNIT_HOUR)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(511));
	vb.set("Nj", Value::createInteger(415));
	vb.set("latfirst", Value::createInteger(-16125000));
	vb.set("latlast", Value::createInteger(9750000));
	vb.set("lonfirst", Value::createInteger(344250000));
	vb.set("lonlast", Value::createInteger(16125000));
	vb.set("latp", Value::createInteger(-40000000));
	vb.set("lonp", Value::createInteger(10000000));
	vb.set("rot", Value::createInteger(0));
	vb.set("tn", Value::createInteger(1));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

    // Check proddef
    ensure_md_equals(md, Proddef, "GRIB(mc=ti,mt=0,pf=1,tf=16,tod=4,ty=3)");

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2010, 5, 24, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));

	// No more gribs
	ensure(not scanner.next(md));
}

// Scan a GRIB2 with experimental UTM areas
template<> template<>
void to::test<8>()
{
    Metadata md;
    scan::Grib scanner;
    scanner.open("inbound/calmety_20110215.grib2");
    ensure(scanner.next(md));

    ensure_md_equals(md, Origin, "GRIB2(00200, 00000, 000, 000, 203)");
    ensure_md_equals(md, Product, "GRIB2(200, 0, 200, 33)");
    ensure_md_equals(md, Level, "GRIB2S(103, 0, 10)");
    ensure_md_equals(md, Timerange, "Timedef(0s, 254)");
    ensure_md_equals(md, Area, "GRIB(Ni=90, Nj=52, fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500, tn=32768, utm=1, zone=32)");
    ensure_md_equals(md, Proddef, "GRIB(tod=0)");
    ensure_md_equals(md, Reftime, "2011-02-15T00:00:00Z");
    ensure_md_equals(md, Run, "MINUTE(0)");

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

        ensure_equals(md.get<Timerange>(), Timerange::decodeString("Timedef(0s,254)"));
    }
    {
        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/ninfa_forc.grib2");
        ensure(scanner.next(md));

        ensure_equals(md.get<Timerange>(), Timerange::decodeString("Timedef(3h,254)"));
    }
}

// Check scanning COSMO nudging timeranges
template<> template<>
void to::test<10>()
{
    {
        metadata::Collection mdc;
        scan::scan("inbound/cosmonudging-t2.grib1", mdc);
        ensure_equals(mdc.size(), 35u);
        for (unsigned i = 0; i < 5; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[5], Timerange, "Timedef(0s, 2, 1h)");
        ensure_md_equals(mdc[6], Timerange, "Timedef(0s, 3, 1h)");
        for (unsigned i = 7; i < 13; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[13], Timerange, "Timedef(0s, 1, 12h)");
        for (unsigned i = 14; i < 19; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[19], Timerange, "Timedef(0s, 1, 12h)");
        ensure_md_equals(mdc[20], Timerange, "Timedef(0s, 1, 12h)");
        for (unsigned i = 21; i < 26; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[26], Timerange, "Timedef(0s, 1, 12h)");
        for (unsigned i = 27; i < 35; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s, 0, 12h)");
    }
    {
        metadata::Collection mdc;
        scan::scan("inbound/cosmonudging-t201.grib1", mdc);
        ensure_equals(mdc.size(), 33u);
        ensure_md_equals(mdc[0], Timerange, "Timedef(0s, 0, 12h)");
        ensure_md_equals(mdc[1], Timerange, "Timedef(0s, 0, 12h)");
        ensure_md_equals(mdc[2], Timerange, "Timedef(0s, 0, 12h)");
        for (unsigned i = 3; i < 16; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[16], Timerange, "Timedef(0s, 1, 12h)");
        ensure_md_equals(mdc[17], Timerange, "Timedef(0s, 1, 12h)");
        for (unsigned i = 18; i < 26; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
        ensure_md_equals(mdc[26], Timerange, "Timedef(0s, 2, 1h)");
        for (unsigned i = 27; i < 33; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
    }
    {
        metadata::Collection mdc;
        scan::scan("inbound/cosmonudging-t202.grib1", mdc);
        ensure_equals(mdc.size(), 11u);
        for (unsigned i = 0; i < 11; ++i)
            ensure_md_equals(mdc[i], Timerange, "Timedef(0s,254)");
    }
    {
        metadata::Collection mdc;
        scan::scan("inbound/cosmonudging-t203.grib1", mdc);
        ensure_equals(mdc.size(), 1u);
        ensure_md_equals(mdc[0], Timerange, "Timedef(0s,254)");
    }
}

// Check scanning a GRIB2 with a bug in level scanning code
template<> template<>
void to::test<11>()
{
    metadata::Collection mdc;
    scan::scan("inbound/wronglevel.grib2", mdc);
    ensure_equals(mdc.size(), 1u);
    ensure_equals(mdc[0].get<Level>(), Level::decodeString("GRIB2S(101,-,-)"));
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

// vim:set ts=4 sw=4:
