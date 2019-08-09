#include "arki/metadata/tests.h"
#include "arki/core/file.h"
#include "arki/scan/grib.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/run.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_grib");

void Tests::register_tests() {

// Test validation
add_method("validation", [] {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::grib::validator();

    sys::File fd("inbound/test.grib1", O_RDONLY);
    v.validate_file(fd, 0, 7218);
    v.validate_file(fd, 7218, 34960);
    v.validate_file(fd, 42178, 2234);

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7219));
    wassert_throws(std::runtime_error, v.validate_file(fd, 7217, 34961));
    wassert_throws(std::runtime_error, v.validate_file(fd, 42178, 2235));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 0));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 10));

    fd.close();

    metadata::TestCollection mdc("inbound/test.grib1");
    buf = mdc[0].get_data().read();

    wassert(v.validate_buf(buf.data(), buf.size()));
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));
    wassert_throws(std::runtime_error, v.validate_buf(buf.data(), buf.size()-1));
});

// Scan a GRIB2 with experimental UTM areas
add_method("utm_areas", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    Metadata md;
    scan::LuaGribScanner scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/calmety_20110215.grib2", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);
    md = mds[0];

    wassert(actual(md).contains("origin", "GRIB2(00200, 00000, 000, 000, 203)"));
    wassert(actual(md).contains("product", "GRIB2(200, 0, 200, 33, 5, 0)"));
    wassert(actual(md).contains("level", "GRIB2S(103, 0, 10)"));
    wassert(actual(md).contains("timerange", "Timedef(0s, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=90, Nj=52, fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500, tn=32768, utm=1, zone=32)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=0)"));
    wassert(actual(md).contains("reftime", "2011-02-15T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));
});


#if 0
// Check opening very long GRIB files for scanning
// TODO: needs skipping of there are no holes
// FIXME: cannot reenable it because currently there is no interface for
// opening without scanning, and eccodes takes a long time to skip all those
// null bytes
add_method("bigfile", [] {
    scan::Grib scanner;
    int fd = open("bigfile.grib1", O_WRONLY | O_CREAT, 0644);
    ensure(fd != -1);
    ensure(ftruncate(fd, 0xFFFFFFFF) != -1);
    close(fd);
    wassert(scanner.test_open("bigfile.grib1"));
});
#endif

}

}
