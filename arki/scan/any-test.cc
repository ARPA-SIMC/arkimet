#include "arki/metadata/tests.h"
#include "arki/libconfig.h"
#include "arki/scan/any.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils/compress.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_any");

void Tests::register_tests() {

// Scan a well-known grib file, with no padding between messages
add_method("grib_compact", [] {
    metadata::Collection mdc;
#ifndef HAVE_GRIBAPI
    ensure(not scan::scan("inbound/test.grib1", mdc.inserter_func()));
#else
    vector<uint8_t> buf;

    ensure(scan::scan("inbound/test.grib1", std::make_shared<core::lock::Null>(), mdc.inserter_func()));
    ensure_equals(mdc.size(), 3u);

    // Check the source info
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

    // Check contents
    wassert(actual(mdc[0]).contains("origin", "GRIB1(200, 0, 101)"));
    wassert(actual(mdc[0]).contains("product", "GRIB1(200, 140, 229)"));
    wassert(actual(mdc[0]).contains("level", "GRIB1(1, 0)"));
    wassert(actual(mdc[0]).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(mdc[0]).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));
    wassert(actual(mdc[0]).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(mdc[0]).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert(actual(mdc[0]).contains("run", "MINUTE(13:00)"));

    // Check the source info
    wassert(actual(mdc[1].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 7218, 34960));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

    // Check contents
    wassert(actual(mdc[1]).contains("origin", "GRIB1(80, 255, 100)"));
    wassert(actual(mdc[1]).contains("product", "GRIB1(80, 2, 2)"));
    wassert(actual(mdc[1]).contains("level", "GRIB1(102, 0)"));
    wassert(actual(mdc[1]).contains("timerange", "GRIB1(1)"));
    wassert(actual(mdc[1]).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));
    wassert(actual(mdc[1]).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(mdc[1]).contains("reftime", "2007-07-07T00:00:00Z"));
    wassert(actual(mdc[1]).contains("run", "MINUTE(0)"));

    // Check the source info
    wassert(actual(mdc[2].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 42178, 2234));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

    // Check contents
    wassert(actual(mdc[2]).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(mdc[2]).contains("product", "GRIB1(98, 128, 129)"));
    wassert(actual(mdc[2]).contains("level", "GRIB1(100, 1000)"));
    wassert(actual(mdc[2]).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(mdc[2]).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(mdc[2]).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(mdc[2]).contains("reftime", "2007-10-09T00:00:00Z"));
    wassert(actual(mdc[2]).contains("run", "MINUTE(0)"));
#endif
});

// Scan a well-known bufr file, with no padding between BUFRs
add_method("bufr_compact", [] {
    metadata::Collection mdc;
#ifndef HAVE_DBALLE
    ensure(not scan::scan("inbound/test.bufr", mdc.inserter_func()));
#else
    vector<uint8_t> buf;

    ensure(scan::scan("inbound/test.bufr", std::make_shared<core::lock::Null>(), mdc.inserter_func()));

	ensure_equals(mdc.size(), 3u);

    // Check the source info
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 0, 194));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

    // Check contents
    wassert(actual(mdc[0]).contains("origin", "BUFR(98, 0)"));
    wassert(actual(mdc[0]).contains("product", "BUFR(0, 255, 1, t=synop)"));
    wassert(actual(mdc[0]).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    wassert(actual(mdc[0]).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(mdc[0]).contains("reftime", "2005-12-01T18:00:00Z"));

	// Check run
	ensure(not mdc[0].has(TYPE_RUN));


    // Check the source info
    wassert(actual(mdc[1].source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 194, 220));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(mdc[1]).contains("origin", "BUFR(98, 0)"));
    wassert(actual(mdc[1]).contains("product", "BUFR(0, 255, 1, t=synop)"));
    //wassert(actual(mdc[1]).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(mdc[1]).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(mdc[1]).contains("reftime", "2004-11-30T12:00:00Z"));

	// Check run
	ensure(not mdc[1].has(TYPE_RUN));


    // Check the source info
    wassert(actual(mdc[2].source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 414, 220));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(mdc[2]).contains("origin", "BUFR(98, 0)"));
    wassert(actual(mdc[2]).contains("product", "BUFR(0, 255, 3, t=synop)"));
    //wassert(actual(mdc[2]).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(mdc[2]).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(mdc[2]).contains("reftime", "2004-11-30T12:00:00Z"));

    // Check run
    ensure(not mdc[2].has(TYPE_RUN));
#endif
});

// Test compression
add_method("compress", [] {
    // Create a test file with 9 gribs inside
    system("cat inbound/test.grib1 inbound/test.grib1 inbound/test.grib1 > a.grib1");
    system("cp a.grib1 b.grib1");

    // Compress
    scan::compress("b.grib1", std::make_shared<core::lock::Null>(), 5);
    sys::unlink_ifexists("b.grib1");

    {
        utils::compress::TempUnzip tu("b.grib1");
        unsigned count = 0;
        scan::scan("b.grib1", std::make_shared<core::lock::Null>(), [&](unique_ptr<Metadata>) { ++count; return true; });
        ensure_equals(count, 9u);
    }
});

// Test reading update sequence numbers
add_method("usn", [] {
#ifdef HAVE_DBALLE
    {
        // Gribs don't have update sequence numbrs, and the usn parameter must
        // be left untouched
        metadata::Collection mdc;
        scan::scan("inbound/test.grib1", std::make_shared<core::lock::Null>(), mdc.inserter_func());
        int usn = 42;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), false);
        ensure_equals(usn, 42);
    }

    {
        metadata::Collection mdc;
        scan::scan("inbound/synop-gts.bufr", std::make_shared<core::lock::Null>(), mdc.inserter_func());
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 0);
    }

    {
        metadata::Collection mdc;
        scan::scan("inbound/synop-gts-usn2.bufr", std::make_shared<core::lock::Null>(), mdc.inserter_func());
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 2);
    }
#endif
});

}

}
