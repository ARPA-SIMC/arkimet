#include "reader.h"
#include "arki/tests/tests.h"
#include "arki/types/source/blob.h"
#include "arki/metadata/collection.h"
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

void test_read(const char* pathname)
{
    auto reader = Reader::create_new(pathname, core::lock::policy_null);
    auto src = types::source::Blob::create("grib", "", pathname, 0, 7218, reader);
    vector<uint8_t> buf = src->read_data();
    wassert(actual(string((const char*)buf.data(), 4)) == "GRIB");
    wassert(actual(string((const char*)buf.data() + 7214, 4)) == "7777");

    src = types::source::Blob::create("grib", "", pathname, 7218, 34960, reader);
    buf = src->read_data();
    wassert(actual(string((const char*)buf.data(), 4)) == "GRIB");
    wassert(actual(string((const char*)buf.data() + 34956, 4)) == "7777");
}


class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_reader");

void Tests::register_tests() {

// Read an uncompressed file
add_method("uncompressed", [] {
    wassert(test_read("inbound/test.grib1"));
});

// Read an uncompressed file without index
add_method("uncompressed_noidx", [] {
    sys::unlink_ifexists("testcompr.grib1");
    sys::unlink_ifexists("testcompr.grib1.gz");
    ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);
    ensure(system("gzip testcompr.grib1") == 0);

    wassert(test_read("testcompr.grib1"));
});

// Read an uncompressed file with the index
add_method("uncompressed_idx", [] {
    sys::unlink_ifexists("testcompr.grib1");
    sys::unlink_ifexists("testcompr.grib1.gz");
    ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);
    scan::compress("testcompr.grib1", core::lock::policy_null, 2);
    sys::unlink_ifexists("testcompr.grib1");
    wassert(test_read("testcompr.grib1"));
});

// Don't segfault on nonexisting files
add_method("missing", [] {
    vector<uint8_t> buf(7218);
    sys::unlink_ifexists("test.grib1");
    auto reader = Reader::create_new("test.grib1", core::lock::policy_null);
    auto src = types::source::Blob::create("grib", "", "test,grib1", 0, 7218, reader);
    try {
        vector<uint8_t> buf = src->read_data();
        ensure(false);
    } catch (std::exception& e) {
        ensure(string(e.what()).find("file does not exist") != string::npos);
    }
});

}

}
