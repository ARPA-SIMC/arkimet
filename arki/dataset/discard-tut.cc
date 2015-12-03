#include "config.h"

#include <arki/tests/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_dataset_discard_shar {
	ConfigFile config;

	arki_dataset_discard_shar()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = discard\n"
			"filter = origin: GRIB1,200\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = discard\n"
			"filter = origin: GRIB1,80\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[error]\n"
			"type = discard\n"
			"name = error\n"
			"path = error\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_dataset_discard);

// Test acquiring the data
template<> template<>
void to::test<1>()
{
	// Import data into the datasets
	Metadata md;
	metadata::Collection mdc;
	scan::Grib scanner;
	RealDispatcher dispatcher(config);
	scanner.open("inbound/test.grib1");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_OK);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 1u);
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_OK);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 2u);
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_ERROR);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 3u);
	ensure(!scanner.next(md));
	dispatcher.flush();
}

}

// vim:set ts=4 sw=4:
