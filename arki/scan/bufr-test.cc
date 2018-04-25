#include "arki/metadata/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/scan/bufr.h"
#include "arki/utils/sys.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_bufr");

void Tests::register_tests() {

// Scan a well-known bufr file, with no padding between BUFRs
add_method("contiguous", [] {
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/test.bufr");

    // See how we scan the first BUFR
    wassert(actual(scanner.next(md)).istrue());

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 0, 194));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 1, t=synop)"));
    wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2005-12-01T18:00:00Z"));

    // Check run
    wassert_true(not md.has(TYPE_RUN));


    // Next bufr
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 194, 220));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 1, t=synop)"));
    //wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2004-11-30T12:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));


    // Last bufr
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/test.bufr", 414, 220));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 3, t=synop)"));
    //wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2004-11-30T12:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));

    // No more bufrs
    wassert_false(scanner.next(md));
});

// Scan a well-known bufr file, with extra padding data between messages
add_method("padded", [] {
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/padded.bufr");

    // See how we scan the first BUFR
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/padded.bufr", 100, 194));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 1, t=synop)"));
    wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2005-12-01T18:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));


    // Next bufr
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/padded.bufr", 394, 220));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 1, t=synop)"));
    //wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2004-11-30T12:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));


    // Last bufr
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/padded.bufr", 714, 220));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(0, 255, 3, t=synop)"));
    //wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2004-11-30T12:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));

    // No more bufrs
    wassert_false(scanner.next(md));
});

// Test validation
add_method("validate", [] {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::bufr::validator();

    sys::File fd("inbound/test.bufr", O_RDONLY);
    v.validate_file(fd, 0, 194);
    v.validate_file(fd, 194, 220);
    v.validate_file(fd, 414, 220);

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 193));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 193));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 195));
    wassert_throws(std::runtime_error, v.validate_file(fd, 193, 221));
    wassert_throws(std::runtime_error, v.validate_file(fd, 414, 221));
    wassert_throws(std::runtime_error, v.validate_file(fd, 634, 0));
    wassert_throws(std::runtime_error, v.validate_file(fd, 634, 10));
    fd.close();

    metadata::TestCollection mdc("inbound/test.bufr");
    buf = mdc[0].getData();

    wassert(v.validate_buf(buf.data(), buf.size()));
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));
    wassert_throws(std::runtime_error, v.validate_buf(buf.data(), buf.size()-1));
});

// Test scanning a BUFR file that can only be decoded partially
// (note: it can now be fully decoded)
add_method("partial", [] {
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/C23000.bufr");

    // See how we scan the first BUFR
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/C23000.bufr", 0, 2206));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2206u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 2202, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(2, 255, 101, t=temp)"));
    //wassert(actual(md).contains("area", "GRIB(lat=4153000, lon=2070000)"));
    //wassert(actual(md).contains("proddef", "GRIB(blo=13, sta=577)"));
    wassert(actual(md).contains("reftime", "2010-07-21T23:00:00Z"));

    // Check area
    wassert_true(md.has(TYPE_AREA));

    // Check run
    wassert_false(md.has(TYPE_RUN));


    // No more bufrs
    wassert_false(scanner.next(md));
});

// Test scanning a pollution BUFR file
add_method("pollution", [] {
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/pollution.bufr");

    // See how we scan the first BUFR
    wassert_true(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("bufr", sys::abspath("."), "inbound/pollution.bufr", 0, 178));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 178u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 174, 4), "7777");

    // Check contents
    wassert(actual(md).contains("origin", "BUFR(98, 0)"));
    wassert(actual(md).contains("product", "BUFR(8, 255, 171, t=pollution,p=NO2)"));
    wassert(actual(md).contains("area", "GRIB(lat=4601194, lon=826889)"));
    wassert(actual(md).contains("proddef", "GRIB(gems=IT0002, name=NO_3118_PIEVEVERGONTE)"));
    wassert(actual(md).contains("reftime", "2010-08-08T23:00:00Z"));

    // Check run
    wassert_false(md.has(TYPE_RUN));


    // No more bufrs
    wassert_false(scanner.next(md));
});

// Test scanning a BUFR file with undefined dates
add_method("zerodate", [] {
    Metadata md;
    scan::Bufr scanner;

    scanner.test_open("inbound/zerodate.bufr");

    // Missing datetime info should lead to missing Reftime
    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());
});

// Test scanning a ship
add_method("ship", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/ship.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-11, y=37, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=DHDE)"));
});

// Test scanning an amdar
add_method("amdar", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/amdar.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=21, y=64, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=EU4444)"));
});

// Test scanning an airep
add_method("airep", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/airep.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-54, y=51, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=ACA872)"));
});

// Test scanning an acars
add_method("acars", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/acars.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-88, y=39, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=JBNYR3RA)"));
});

// Test scanning a GTS synop
add_method("gts", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/synop-gts.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(lat=4586878, lon=717080)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=6, sta=717)"));
});

// Test scanning a message with a different date in the header than in its contents
add_method("date_mismatch", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/synop-gts-different-date-in-header.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(lat=4586878, lon=717080)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=6, sta=717)"));
});

// Test scanning a message which raises domain errors when interpreted
add_method("out_of_range", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/interpreted-range.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("Area", "GRIB(type=mob, x=10, y=53)"));
    wassert(actual(md).contains("Proddef", "GRIB(id=DBBC)"));
});

// Test scanning a temp forecast, to see if we got the right reftime
add_method("temp_reftime", [] {
    // BUFR has datetime 2009-02-13 12:00:00, timerange instant
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/tempforecast.bufr");
    wassert_true(scanner.next(md));
    wassert(actual(md).contains("reftime", "2009-02-13 12:00:00"));

    // BUFR has datetime 2013-04-06 00:00:00 (validity time, in this case), timerange 254,259200,0 (+72h)
    // and should be archived with its emission time
    scanner.test_open("inbound/tempforecast1.bufr");
    wassert_true(scanner.next(md));
    wassert(actual(md).contains("reftime", "2013-04-06 00:00:00"));
});

// Test scanning a bufr with all sorts of wrong dates
add_method("wrongdate", [] {
    Metadata md;
    scan::Bufr scanner;
    scanner.test_open("inbound/wrongdate.bufr");

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());

    wassert(actual(scanner.next(md)).isfalse());
});

}

}
