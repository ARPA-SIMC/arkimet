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

#include "config.h"

#include <arki/types/tests.h>
#include <arki/scan/odimh5.h>
#include <arki/metadata.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/reftime.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <arki/types/level.h>
#include <arki/types/product.h>
#include <arki/types/area.h>
#include <arki/types/timerange.h>

#include <wibble/sys/fs.h>

namespace tut {

using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_odimh5_shar
{
};

TESTGRP(arki_scan_odimh5);

// Scan an ODIMH5 polar volume
template<> template<>
void to::test<1>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/PVOL_v20.h5");

	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/PVOL_v20.h5", 0, 320696));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 320696u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("wmo","rad","plc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("PVOL","SCAN")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::ODIMH5::create(0,27)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2000,1,2,3,4,5))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("task")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("ACRR,BRDR,CLASS,DBZH,DBZV,HGHT,KDP,LDR,PHIDP,QIND,RATE,RHOHV,SNR,SQI,TH,TV,UWND,VIL,VRAD,VWND,WRAD,ZDR,ad,ad_dev,chi2,dbz,dbz_dev,dd,dd_dev,def,def_dev,div,div_dev,ff,ff_dev,n,rhohv,rhohv_dev,w,w_dev,z,z_dev")));

	// Check area
	vb.clear();
	vb.set("lat", Value::createInteger(44456700));
	vb.set("lon", Value::createInteger(11623600));
	vb.set("radius", Value::createInteger(1000));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::ODIMH5::create(vb)));

	ensure(not scanner.next(md));
}

template<> template<>
void to::test<2>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_CAPPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_CAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","CAPPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(105, 10)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<3>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_ETOP_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_ETOP_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","ETOP")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("HGHT")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<4>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_LBM_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_LBM_v20.h5", 0, 49057));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49057u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP", "NEW:LBM_ARPA")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<5>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_MAX_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_MAX_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP", "MAX")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<6>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_PCAPPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_PCAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","PCAPPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(105,10)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<7>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_PPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_PPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","PPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::ODIMH5::create(10)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<8>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_RR_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_RR_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","RR")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(types::timerange::Timedef::create(0, types::timerange::Timedef::UNIT_SECOND,
																																												1,
																																												1, types::timerange::Timedef::UNIT_HOUR)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("ACRR")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<9>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/COMP_VIL_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/COMP_VIL_v20.h5", 0, 49097));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49097u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("COMP","VIL")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(106, 10, 0)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("VIL")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<10>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_CAPPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_CAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","CAPPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(105, 500)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<11>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_ETOP_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_ETOP_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","ETOP")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("HGHT")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<12>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_HVMI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_HVMI_v20.h5", 0, 68777));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 68777u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","HVMI")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<13>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_MAX_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_MAX_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","MAX")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<14>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_PCAPPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_PCAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","PCAPPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(105, 500)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<15>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_PPI_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_PPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","PPI")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::ODIMH5::create(10)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<16>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_RR_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_RR_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","RR")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check timerange
	ensure(md.get(types::TYPE_TIMERANGE).defined());
	ensure_equals(md.get(types::TYPE_TIMERANGE), Item<>(types::timerange::Timedef::create(0, types::timerange::Timedef::UNIT_SECOND,
																																												1,
																																												1, types::timerange::Timedef::UNIT_HOUR)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("ACRR")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<17>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_VIL_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_VIL_v20.h5", 0, 49097));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49097u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE","VIL")));

	// Check level
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::GRIB1::create(106, 10, 0)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,14,30,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("VIL")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}
template<> template<>
void to::test<18>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/IMAGE_ZLR-BB_v20.h5");
	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source).sourceblob_is("odimh5", sys::fs::abspath("."), "inbound/odimh5/IMAGE_ZLR-BB_v20.h5", 0, 62161));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 62161u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("16144","IY46","itspc")));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("IMAGE", "NEW:LBM_ARPA")));

	// Check level
	ensure(!md.get(types::TYPE_LEVEL).defined());

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2013,3,18,10,0,0))));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("ZLR-BB")));

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("DBZH")));

	// Check area
	vb.clear();
	vb.set("latfirst", Value::createInteger(42314117));
	vb.set("lonfirst", Value::createInteger(8273203));
	vb.set("latlast", Value::createInteger(46912151));
	vb.set("lonlast", Value::createInteger(14987079));
	vb.set("type", Value::createInteger(0));
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	ensure(not scanner.next(md));
}

// Check that the scanner silently discard an empty file
template<> template<>
void to::test<19>()
{
	Metadata md;
	scan::OdimH5 scanner;
	wibble::sys::Buffer buf;
	ValueBag vb;

	scanner.open("inbound/odimh5/empty.h5");
	ensure(not scanner.next(md));
}

}

// vim:set ts=4 sw=4:
