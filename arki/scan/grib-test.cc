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

void is_grib_data(Metadata& md, size_t expected_size)
{
    auto buf = wcallchecked(md.get_data().read());
    wassert(actual(buf.size()) == expected_size);
    wassert(actual(string((const char*)buf.data(), 4)) == "GRIB");
    wassert(actual(string((const char*)buf.data() + expected_size - 4, 4)) == "7777");
}

void Tests::register_tests() {

add_method("lua_results", [] {
    scan::LuaGribScanner scanner("", R"(
arki.year = 2008
arki.month = 7
arki.day = 30
arki.hour = 12
arki.minute = 30
arki.second = 0
arki.centre = 98
arki.subcentre = 1
arki.process = 2
arki.origin = 1
arki.table = 1
arki.product = 1
arki.ltype = 1
arki.l1 = 1
arki.l2 = 1
arki.ptype = 1
arki.punit = 1
arki.p1 = 1
arki.p2 = 1
arki.bbox = { { 45.00, 11.00 }, { 46.00, 11.00 }, { 46.00, 12.00 }, { 47.00, 13.00 }, { 45.00, 12.00 } }
)");
    metadata::Collection mds;
    scanner.test_scan_file("inbound/test.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 3u);

    // Check the source info
    wassert(actual(mds[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));
});

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

// Check scanning of some Timedef cases
add_method("ninfa", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    {
        scan::LuaGribScanner scanner;
        metadata::Collection mds;
        scanner.test_scan_file("inbound/ninfa_ana.grib2", mds.inserter_func());
        wassert(actual(mds.size()) == 1u);
        wassert(actual(mds[0]).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        scan::LuaGribScanner scanner;
        metadata::Collection mds;
        scanner.test_scan_file("inbound/ninfa_forc.grib2", mds.inserter_func());
        wassert(actual(mds.size()) == 1u);
        wassert(actual(mds[0]).contains("timerange", "Timedef(3h,254,0s)"));
    }
});

// Check scanning COSMO nudging timeranges
add_method("cosmo_nudging", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    ARKI_UTILS_TEST_INFO(info);

    {
        metadata::TestCollection mdc("inbound/cosmonudging-t2.grib1");
        wassert(actual(mdc.size()) == 35u);
        for (unsigned i = 0; i < 5; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[5]).contains("timerange", "Timedef(0s, 2, 1h)"));
        wassert(actual(mdc[6]).contains("timerange", "Timedef(0s, 3, 1h)"));
        for (unsigned i = 7; i < 13; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[13]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 14; i < 19; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[19]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[20]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 21; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 27; i < 35; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s, 0, 12h)"));
    }
    {
        metadata::TestCollection mdc("inbound/cosmonudging-t201.grib1");
        wassert(actual(mdc.size()) == 33u);
        wassert(actual(mdc[0]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[1]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[2]).contains("timerange", "Timedef(0s, 0, 12h)"));
        for (unsigned i = 3; i < 16; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[16]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[17]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 18; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 2, 1h)"));
        for (unsigned i = 27; i < 33; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        metadata::TestCollection mdc("inbound/cosmonudging-t202.grib1");
        wassert(actual(mdc.size()) == 11u);
        for (unsigned i = 0; i < 11; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }


    // Shortcut to read one single GRIB from a file
    struct OneGrib
    {
        arki::utils::tests::LocationInfo& arki_utils_test_location_info;
        Metadata md;

        OneGrib(arki::utils::tests::LocationInfo& info) : arki_utils_test_location_info(info) {}
        void read(const char* fname)
        {
            arki_utils_test_location_info() << "Sample: " << fname;
            metadata::TestCollection mdc(fname);
            wassert(actual(mdc.size()) == 1u);
            md = wcallchecked(mdc[0]);
        }
    };

    OneGrib md(info);

    md.read("inbound/cosmonudging-t203.grib1");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));

    md.read("inbound/cosmo/anist_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fc0ist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == std::string("Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/anproc_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_3.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,2,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_4.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,12h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fcist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,1,6h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_3.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(48h,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));
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
