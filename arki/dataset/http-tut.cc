#include "config.h"

#include <arki/tests/tests.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
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

namespace {
inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}
}

struct arki_dataset_http_shar {
	ConfigFile config;

	arki_dataset_http_shar()
	{
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
        metadata::Collection mdc;
        scan::Grib scanner;
        RealDispatcher dispatcher(config);
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_ERROR);
        ensure(!scanner.next(md));
        dispatcher.flush();
    }
};
TESTGRP(arki_dataset_http);

// Test allSameRemoteServer
template<> template<>
void to::test<1>()
{
	using namespace arki::dataset;

	{
		string conf =
			"[test200]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/test200\n"
			"\n"
			"[test80]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/test80\n"
			"\n"
			"[error]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/error\n";
		stringstream incfg(conf);
		ConfigFile cfg;
		cfg.parse(incfg, "(memory)");

		ensure_equals(HTTP::allSameRemoteServer(cfg), "http://foo.bar/foo");
	}

	{
		string conf =
			"[test200]\n"
			"type = remote\n"
			"path = http://bar.foo.bar/foo/dataset/test200\n"
			"\n"
			"[test80]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/test80\n"
			"\n"
			"[error]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/error\n";
		stringstream incfg(conf);
		ConfigFile cfg;
		cfg.parse(incfg, "(memory)");

		ensure_equals(HTTP::allSameRemoteServer(cfg), "");
	}

	{
		string conf =
			"[test200]\n"
			"type = ondisk2\n"
			"path = http://foo.bar/foo/dataset/test200\n"
			"\n"
			"[test80]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/test80\n"
			"\n"
			"[error]\n"
			"type = remote\n"
			"path = http://foo.bar/foo/dataset/error\n";
		stringstream incfg(conf);
		ConfigFile cfg;
		cfg.parse(incfg, "(memory)");

		ensure_equals(HTTP::allSameRemoteServer(cfg), "");
	}
#if 0
	unique_ptr<Reader> testds(Reader::create(*config.section("test200")));
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
