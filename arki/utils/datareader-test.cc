#include "config.h"
#include "arki/tests/tests.h"
#include "arki/metadata/collection.h"
#include "arki/utils/datareader.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_datareader");

void Tests::register_tests() {

// Read an uncompressed file
add_method("uncompressed", [] {
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
});

// Read an uncompressed file without index
add_method("uncompressed_noidx", [] {
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
});

// Read an uncompressed file with the index
add_method("uncompressed_idx", [] {
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
});

// Don't segfault on nonexisting files
add_method("missing", [] {
    utils::DataReader dr;
    vector<uint8_t> buf(7218);
    sys::unlink_ifexists("test.grib1");
	try {
		dr.read("test.grib1", 0, 7218, buf.data());
		ensure(false);
	} catch (std::exception& e) {
		ensure(string(e.what()).find("file does not exist") != string::npos);
	}
});

}

}
