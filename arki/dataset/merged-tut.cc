#include "config.h"
#include <arki/tests/tests.h>
#include <arki/dataset/merged.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_dataset_merged_shar {
    ConfigFile config;
    dataset::Reader* ds1;
    dataset::Reader* ds2;
    dataset::Reader* ds3;
    dataset::Merged ds;

	arki_dataset_merged_shar() : ds1(0), ds2(0), ds3(0)
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

        // In-memory dataset configuration
        string conf =
            "[test200]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "index = origin, reftime\n"
            "name = test200\n"
            "path = test200\n"
            "\n"
            "[test80]\n"
            "type = ondisk2\n"
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
        config.parse(conf, "(memory)");

        // Import data into the datasets
        Metadata md;
        metadata::Collection mdc;
        scan::Grib scanner;
        RealDispatcher dispatcher(config);
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_ERROR);
        ensure(!scanner.next(md));
        dispatcher.flush();

        ds.addDataset(*(ds1 = dataset::Reader::create(*config.section("test200")).release()));
        ds.addDataset(*(ds2 = dataset::Reader::create(*config.section("test80")).release()));
        ds.addDataset(*(ds3 = dataset::Reader::create(*config.section("error")).release()));
    }

	~arki_dataset_merged_shar()
	{
		if (ds1) delete ds1;
		if (ds2) delete ds2;
		if (ds3) delete ds3;
	}
};
TESTGRP(arki_dataset_merged);

// Test querying the datasets
def_test(1)
{
    metadata::Collection mdc(ds, Matcher());
    ensure_equals(mdc.size(), 3u);

#if 0
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));
    metadata::Collection mdc;

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
