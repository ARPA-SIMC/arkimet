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
#if 0
	ReadonlyDataset* ds1;
	ReadonlyDataset* ds2;
	ReadonlyDataset* ds3;
#endif
	//dataset::Gridspace ds;

	arki_dataset_gridspace_shar() // : ds1(0), ds2(0), ds3(0)
	{
#if 0
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"name = test80\n"
			"path = test80\n"
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
		ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_ERROR);
		ensure(!scanner.next(md));
		dispatcher.flush();

		ds.addDataset(*(ds1 = ReadonlyDataset::create(*config.section("test200"))));
		ds.addDataset(*(ds2 = ReadonlyDataset::create(*config.section("test80"))));
		ds.addDataset(*(ds3 = ReadonlyDataset::create(*config.section("error"))));
#endif
	}

	~arki_dataset_gridspace_shar()
	{
#if 0
		if (ds1) delete ds1;
		if (ds2) delete ds2;
		if (ds3) delete ds3;
#endif
	}
};
TESTGRP(arki_dataset_gridspace);

// Test querying the datasets
template<> template<>
void to::test<1>()
{
#if 0
	MetadataCollector mdc;
	ds.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);
#endif

#if 0
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
	MetadataCollector mdc;

	testds->query(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	Source source = mdc[0].source;
	ensure_equals(source.style, Source::BLOB);
	ensure_equals(source.format, "grib1");
	string fname;
	size_t offset, len;
	source.getBlob(fname, offset, len);
	ensure_equals(fname, "test200/2007/07-08.grib1");
	ensure_equals(offset, 0u);
	ensure_equals(len, 7218u);

	mdc.clear();
	testds->query(Matcher::parse("origin:GRIB1,80"), false, mdc);
	ensure_equals(mdc.size(), 0u);

	mdc.clear();
	testds->query(Matcher::parse("origin:GRIB1,98"), false, mdc);
	ensure_equals(mdc.size(), 0u);
#endif
}

}

// vim:set ts=4 sw=4:
