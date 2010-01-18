/*
 * Copyright (C) 2010  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/dataset/gridspace.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

struct arki_dataset_gridspace_shar {
	ConfigFile config;
	ReadonlyDataset* ds;
	dataset::Gridspace* gs;

	arki_dataset_gridspace_shar() : ds(0), gs(0)
	{
		// Cleanup the test datasets
		system("rm -rf testds ; mkdir testds");
		system("rm -rf error ; mkdir error");

		// In-memory dataset configuration
		string conf =
			"[testds]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1\n"
			"index = origin, reftime\n"
			"name = testds\n"
			"path = testds\n"
			"\n"
			"[error]\n"
			"type = error\n"
			"step = daily\n"
			"name = error\n"
			"path = error\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");

		// Import data into the datasets
		Metadata md;
		MetadataCollector mdc;
		scan::Grib scanner;
		RealDispatcher dispatcher(config);
		scanner.open("inbound/test.grib1");
		ensure(scanner.next(md));
		ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
		ensure(scanner.next(md));
		ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
		ensure(scanner.next(md));
		ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
		ensure(!scanner.next(md));
		dispatcher.flush();

		ds = ReadonlyDataset::create(*config.section("testds"));
		gs = new dataset::Gridspace(*ds);
	}

	~arki_dataset_gridspace_shar()
	{
		if (gs) delete gs;
		if (ds) delete ds;
	}
};
TESTGRP(arki_dataset_gridspace);


// Test querying the datasets
template<> template<>
void to::test<1>()
{
	// Trivially query only one item
	gs->clear();
	gs->add(types::origin::GRIB1::create(200, 0, 101));
	gs->add(types::product::GRIB1::create(200, 140, 229));
	gs->validate();

	MetadataCollector mdc;
	gs->queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	gs->queryData(dataset::DataQuery(Matcher(), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying the datasets, using matchers
template<> template<>
void to::test<2>()
{
	// Trivially query only one item
	gs->clear();
	gs->addOne(types::TYPE_ORIGIN, "GRIB1,200,0,101");
	gs->addOne(types::TYPE_PRODUCT, "GRIB1,200,140,229");
	gs->validate();

	MetadataCollector mdc;
	gs->queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	gs->queryData(dataset::DataQuery(Matcher(), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying the datasets, using matchers
template<> template<>
void to::test<3>()
{
	// Trivially query only one item
	gs->clear();
	gs->addAll(types::TYPE_ORIGIN, "GRIB1,200,0,101");
	gs->addAll(types::TYPE_PRODUCT, "GRIB1,200,140,229");
	gs->validate();

	MetadataCollector mdc;
	gs->queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	gs->queryData(dataset::DataQuery(Matcher(), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

}

// vim:set ts=4 sw=4:
