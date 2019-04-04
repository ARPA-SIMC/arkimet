#include "arki/metadata/tests.h"
#include "arki/libconfig.h"
#include "arki/scan.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct TestData
{
    std::string pathname;
    unsigned count;
    std::string format;

    TestData(const std::string& pathname, unsigned count=1)
        : pathname(pathname), count(count), format(scan::Scanner::format_from_filename(pathname)) {}
};

class Tests : public TestCase
{
    using TestCase::TestCase;
    std::vector<TestData> test_data;

    void register_tests() override;
} test("arki_scan");

void Tests::register_tests() {
    test_data.emplace_back("inbound/fixture.grib1", 3);
    test_data.emplace_back("inbound/fixture.bufr", 3);
    test_data.emplace_back("inbound/fixture.vm2", 3);
    test_data.emplace_back("inbound/fixture.odimh5/00.odimh5", 1);

// Test format_from_ext
add_method("format_from_filename", [] {
    wassert(actual(scan::Scanner::format_from_filename("test.grib")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib1")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib2")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.bufr")) == "bufr");
    wassert(actual(scan::Scanner::format_from_filename("test.grib.gz")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.gz")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.gz")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.gz")) == "bufr");
    wassert(actual(scan::Scanner::format_from_filename("test.grib.zip")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.zip")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.zip")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.zip")) == "bufr");
    wassert(actual(scan::Scanner::format_from_filename("test.grib.tar")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.tar")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.tar")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.tar")) == "bufr");
#ifdef HAVE_HDF5
    wassert(actual(scan::Scanner::format_from_filename("test.h5")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odim")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.h5.gz")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.gz")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odim.gz")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.gz")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.h5.zip")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.zip")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odim.zip")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.zip")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.h5.tar")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.tar")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odim.tar")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.tar")) == "odimh5");
#endif
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.zip"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.tar"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.gz"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.foo"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.foo.zip"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.foo.tar"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.foo.gz"));
});

for (auto td: test_data)
{
    add_method("scan_file_" + td.format, [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        metadata::Collection mds;
        scanner->test_scan_file(td.pathname, mds.inserter_func());
        wassert(actual(mds.size()) == td.count);
        wassert(actual(mds[0].source().style()) == types::Source::BLOB);
    });

    add_method("scan_pipe_" + td.format, [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        metadata::Collection mds;
        sys::File in(td.pathname, O_RDONLY);
        scanner->scan_pipe(in, mds.inserter_func());
        wassert(actual(mds.size()) == td.count);
        for (const auto& md: mds)
        {
            wassert(actual(md->source().style()) == types::Source::INLINE);
            wassert(actual(md->source().format) == td.format);
        }
    });
}

for (auto td: {TestData("inbound/ship.bufr"), TestData("inbound/oddunits.grib"), TestData("inbound/odimh5/XSEC_v21.h5"), TestData("inbound/single.vm2")})
{
    add_method("scan_singleton_" + td.format, [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        Metadata md;
        scanner->scan_singleton(td.pathname, md);
        wassert_false(md.has_source());
    });
}

// Test reading update sequence numbers
add_method("usn", [] {
#ifdef HAVE_DBALLE
    {
        // Gribs don't have update sequence numbrs, and the usn parameter must
        // be left untouched
        metadata::TestCollection mdc("inbound/test.grib1");
        int usn = 42;
        ensure_equals(scan::Scanner::update_sequence_number(mdc[0], usn), false);
        ensure_equals(usn, 42);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts.bufr");
        int usn;
        ensure_equals(scan::Scanner::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 0);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts-usn2.bufr");
        int usn;
        ensure_equals(scan::Scanner::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 2);
    }
#endif
});

}

}
