/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/scan/grib.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

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
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(13, 254, 0, 0)));

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

	// Check ensemble
	vb.clear();
	ensure(!md.get(types::TYPE_ENSEMBLE).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2009, 9, 2, 0, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(0)));

	// No more gribs
	ensure(not scanner.next(md));
}


}

// vim:set ts=4 sw=4:
