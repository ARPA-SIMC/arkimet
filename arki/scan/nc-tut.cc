#include <arki/metadata/tests.h>
#include <arki/scan/nc.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_nc_shar {
};
TESTGRP(arki_scan_nc);

// Scan a well-known nc sample
template<> template<>
void to::test<1>()
{
    Metadata md;
    scan::NetCDF scanner;
    vector<uint8_t> buf;

    scanner.open("inbound/example_1.nc");
    // See how we scan the first vm2
    wassert(actual(scanner.next(md)).istrue());

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test.vm2", 0, 34));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(227)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:00Z"));

    // FIXME: work in progress
}

}
