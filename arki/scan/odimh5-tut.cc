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

#include <wibble/sys/fs.h>

#include <radarlib/radar.hpp>

namespace tut {

/*============================================================================*/

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_odimh5_shar
{
};

TESTGRP(arki_scan_odimh5);

static const std::string pvol001path 	= "inbound/pvol001.h5";

template<> template<> void to::test<1>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check the source info (check filesize!)
	size_t filesize = Radar::FileSystem::getFileSize(pvol001abspath);
	ensure_equals(
		md.source,
		Item<Source>(source::Blob::create("odimh5", pvol001abspath, 0, filesize))
	);

	ensure(not scanner.next(md));
}


template<> template<> void to::test<2>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check PRODUCT
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(
		md.get(types::TYPE_PRODUCT),
		Item<>(product::ODIMH5::create("PVOL","SCAN"))
	);

	ensure(not scanner.next(md));
}


template<> template<> void to::test<3>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(
		md.get(types::TYPE_ORIGIN),
		Item<>(origin::ODIMH5::create("wmo","rad","plc"))
	);

	ensure(not scanner.next(md));
}

template<> template<> void to::test<4>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(
		reftime::Position::create(types::Time::create(2000,01,02,03,04,05))
	));

	ensure(not scanner.next(md));
}

template<> template<> void to::test<5>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check task
	ensure(md.get(types::TYPE_TASK).defined());
	ensure_equals(md.get(types::TYPE_TASK), Item<>(
		types::Task::create("task")
	));

	ensure(not scanner.next(md));
}

template<> template<> void to::test<6>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	std::set<std::string>	quantities;

	OdimH5v20::Specification::getStandardQuantities(quantities);

	std::string values;
	for (std::set<std::string>::iterator i=quantities.begin(); i!=quantities.end(); i++)
	{
		if (values.size())
			values += ",";
		values += *i;
	}

	// Check quantities
	ensure(md.get(types::TYPE_QUANTITY).defined());
	ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(
		types::Quantity::create(values)
	));

	ensure(not scanner.next(md));
}

template<> template<> void to::test<7>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	// Check elevations (ANCHE CON LE RIPETIZIONI DEVE FUNZIONARE)
	ensure(md.get(types::TYPE_LEVEL).defined());
	ensure_equals(md.get(types::TYPE_LEVEL), Item<>(
		types::level::ODIMH5::create(0,27)
	));

	ensure(not scanner.next(md));
}

template<> template<> void to::test<8>()
{
	Metadata 	md;
	scan::OdimH5 	scanner;
	std::string 	pvol001abspath 	= sys::fs::abspath(pvol001path);

	scanner.open(pvol001path);

	ensure(scanner.next(md));

	//mi aspetto che valori di san pietro capofiume convertiti in interi 
	ValueBag areaValues;
	areaValues.set("lon",    Value::createInteger((int)(44.6547 * 1000000.)));
	areaValues.set("lat",    Value::createInteger((int)(11.6236 * 1000000.)));
	areaValues.set("radius", Value::createInteger((int)(1000.   * 1000.)));

	md.set(types::area::ODIMH5::create(areaValues));

	// Check elevations (ANCHE CON LE RIPETIZIONI DEVE FUNZIONARE)
	ensure(md.get(types::TYPE_AREA).defined());
	ensure_equals(md.get(types::TYPE_AREA), Item<>(
		types::area::ODIMH5::create(areaValues)
	));

	ensure(not scanner.next(md));
}

/*============================================================================*/

}

// vim:set ts=4 sw=4:
