#include <arki/metadata/tests.h>
#include <arki/types/tests.h>
#include <arki/scan/bufr.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/run.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include <sstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_bufr_shar {
};
TESTGRP(arki_scan_bufr);

// Scan a well-known bufr file, with no padding between BUFRs
def_test(1)
{
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

	scanner.open("inbound/test.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// Next bufr
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// Last bufr
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// No more bufrs
	ensure(not scanner.next(md));
}


// Scan a well-known bufr file, with extra padding data between messages
def_test(2)
{
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

	scanner.open("inbound/padded.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// Next bufr
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// Last bufr
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));

	// No more bufrs
	ensure(not scanner.next(md));
}

// Test validation
def_test(3)
{
    Metadata md;
    vector<uint8_t> buf;

	const scan::Validator& v = scan::bufr::validator();

	int fd = open("inbound/test.bufr", O_RDONLY);

	v.validate(fd, 0, 194, "inbound/test.bufr");
	v.validate(fd, 194, 220, "inbound/test.bufr");
	v.validate(fd, 414, 220, "inbound/test.bufr");

#define ensure_throws(x) do { try { x; ensure(false); } catch (std::exception& e) { } } while (0)

	ensure_throws(v.validate(fd, 1, 193, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 0, 193, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 0, 195, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 193, 221, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 414, 221, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 634, 0, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 634, 10, "inbound/test.bufr"));

	close(fd);

    metadata::Collection mdc;
    scan::scan("inbound/test.bufr", mdc.inserter_func());
    buf = mdc[0].getData();

	v.validate(buf.data(), buf.size());
	ensure_throws(v.validate((const char*)buf.data()+1, buf.size()-1));
	ensure_throws(v.validate(buf.data(), buf.size()-1));
}

// Test scanning a BUFR file that can only be decoded partially
// (note: it can now be fully decoded)
def_test(4)
{
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

	scanner.open("inbound/C23000.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

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
	ensure(md.has(TYPE_AREA));

	// Check run
	ensure(not md.has(TYPE_RUN));


	// No more bufrs
	ensure(not scanner.next(md));
}

// Test scanning a pollution BUFR file
def_test(5)
{
    Metadata md;
    scan::Bufr scanner;
    vector<uint8_t> buf;

	scanner.open("inbound/pollution.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

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
	ensure(not md.has(TYPE_RUN));


	// No more bufrs
	ensure(not scanner.next(md));
}

// Test scanning a BUFR file with undefined dates
def_test(6)
{
    Metadata md;
    scan::Bufr scanner;

    scanner.open("inbound/zerodate.bufr");

    // Missing datetime info should lead to missing Reftime
    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(md.get<Reftime>()).isfalse());
}

// Test scanning a ship
def_test(7)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/ship.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-11, y=37, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=DHDE)"));
}

// Test scanning an amdar
def_test(8)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/amdar.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=21, y=64, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=EU4444)"));
}

// Test scanning an airep
def_test(9)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/airep.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-54, y=51, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=ACA872)"));
}

// Test scanning an acars
def_test(10)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/acars.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(x=-88, y=39, type=mob)"));
    wassert(actual(md).contains("proddef", "GRIB(id=JBNYR3RA)"));
}

// Test scanning a GTS synop
def_test(11)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/synop-gts.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(lat=4586878, lon=717080)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=6, sta=717)"));
}

// Test scanning a message with a different date in the header than in its contents
def_test(12)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/synop-gts-different-date-in-header.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("area", "GRIB(lat=4586878, lon=717080)"));
    wassert(actual(md).contains("proddef", "GRIB(blo=6, sta=717)"));
}

// Test scanning a message which raises domain errors when interpreted
def_test(13)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/interpreted-range.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("Area", "GRIB(type=mob, x=10, y=53)"));
    wassert(actual(md).contains("Proddef", "GRIB(id=DBBC)"));
}

// Test scanning a temp forecast, to see if we got the right reftime
def_test(14)
{
    // BUFR has datetime 2009-02-13 12:00:00, timerange instant
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/tempforecast.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("reftime", "2009-02-13 12:00:00"));

    // BUFR has datetime 2013-04-06 00:00:00 (validity time, in this case), timerange 254,259200,0 (+72h)
    // and should be archived with its emission time
    scanner.open("inbound/tempforecast1.bufr");
    ensure(scanner.next(md));
    wassert(actual(md).contains("reftime", "2013-04-03 00:00:00"));
}

// Test scanning a bufr with all sorts of wrong dates
def_test(15)
{
    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/wrongdate.bufr");

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
}

}

// vim:set ts=4 sw=4:
