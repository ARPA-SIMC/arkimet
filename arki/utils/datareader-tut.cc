#include "config.h"
#include <arki/tests/tests.h>
#include <arki/metadata/collection.h>
#include <arki/utils/datareader.h>
#include <arki/utils/sys.h>
#include <arki/scan/any.h>
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

struct arki_utils_datareader_shar {
};
TESTGRP(arki_utils_datareader);

// Read an uncompressed file
template<> template<>
void to::test<1>()
{
    utils::DataReader dr;

    vector<uint8_t> buf;
    buf.resize(7218);
    dr.read("inbound/test.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("inbound/test.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Read an uncompressed file without index
template<> template<>
void to::test<2>()
{
    utils::DataReader dr;

    sys::unlink_ifexists("testcompr.grib1");
    sys::unlink_ifexists("testcompr.grib1.gz");
    ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);
    ensure(system("gzip testcompr.grib1") == 0);

    vector<uint8_t> buf;
    buf.resize(7218);
    dr.read("testcompr.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("testcompr.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Read an uncompressed file with the index
template<> template<>
void to::test<3>()
{
    utils::DataReader dr;

    sys::unlink_ifexists("testcompr.grib1");
    sys::unlink_ifexists("testcompr.grib1.gz");
    ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);

    metadata::Collection mdc;
    scan::scan("testcompr.grib1", mdc.inserter_func());
    mdc.compressDataFile(2, "testcompr.grib1");
    sys::unlink_ifexists("testcompr.grib1");

    vector<uint8_t> buf;
    buf.resize(7218);
    dr.read("testcompr.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("testcompr.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Don't segfault on nonexisting files
template<> template<>
void to::test<4>()
{
    utils::DataReader dr;
    vector<uint8_t> buf(7218);
    sys::unlink_ifexists("test.grib1");
	try {
		dr.read("test.grib1", 0, 7218, buf.data());
		ensure(false);
	} catch (std::exception& e) {
		ensure(string(e.what()).find("file does not exist") != string::npos);
	}
}

}
