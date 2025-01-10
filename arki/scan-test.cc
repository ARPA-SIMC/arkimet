#include "arki/metadata/tests.h"
#include "arki/libconfig.h"
#include "arki/scan.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct TestData
{
    std::string pathname;
    unsigned count;
    DataFormat format;

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
    wassert(actual(scan::Scanner::format_from_filename("test.grib")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib1")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib2")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.bufr")) == DataFormat::BUFR);
    wassert(actual(scan::Scanner::format_from_filename("test.grib.gz")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.gz")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.gz")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.gz")) == DataFormat::BUFR);
    wassert(actual(scan::Scanner::format_from_filename("test.grib.zip")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.zip")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.zip")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.zip")) == DataFormat::BUFR);
    wassert(actual(scan::Scanner::format_from_filename("test.grib.tar")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib1.tar")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.grib2.tar")) == DataFormat::GRIB);
    wassert(actual(scan::Scanner::format_from_filename("test.bufr.tar")) == DataFormat::BUFR);

    wassert(actual(scan::Scanner::format_from_filename("test.h5")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odim")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.h5.gz")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.gz")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odim.gz")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.gz")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.h5.zip")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.zip")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odim.zip")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.zip")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.h5.tar")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5.tar")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odim.tar")) == DataFormat::ODIMH5);
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5.tar")) == DataFormat::ODIMH5);

    wassert(actual(scan::Scanner::format_from_filename("test.nc")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.netcdf")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.nc.gz")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.netcdf.gz")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.nc.zip")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.netcdf.zip")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.nc.tar")) == DataFormat::NETCDF);
    wassert(actual(scan::Scanner::format_from_filename("test.netcdf.tar")) == DataFormat::NETCDF);

    wassert(actual(scan::Scanner::format_from_filename("test.jpg")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpeg")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpg.gz")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpeg.gz")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpg.zip")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpeg.zip")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpg.tar")) == DataFormat::JPEG);
    wassert(actual(scan::Scanner::format_from_filename("test.jpeg.tar")) == DataFormat::JPEG);

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
    add_method("scan_file_" + format_name(td.format), [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        metadata::Collection mds;
        scanner->test_scan_file(td.pathname, mds.inserter_func());
        wassert(actual(mds.size()) == td.count);
        wassert(actual(mds[0].source().style()) == types::Source::Style::BLOB);
    });

    add_method("scan_pipe_" + format_name(td.format), [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        metadata::Collection mds;
        sys::File in(td.pathname, O_RDONLY);
        scanner->scan_pipe(in, mds.inserter_func());
        wassert(actual(mds.size()) == td.count);
        for (const auto& md: mds)
        {
            wassert(actual(md->source().style()) == types::Source::Style::INLINE);
            wassert(actual(md->source().format) == td.format);
        }
    });
}

for (auto td: {TestData("inbound/ship.bufr"), TestData("inbound/oddunits.grib"), TestData("inbound/odimh5/XSEC_v21.h5"), TestData("inbound/single.vm2")})
{
    add_method("scan_singleton_" + format_name(td.format), [=] {
        auto scanner = scan::Scanner::get_scanner(td.format);
        auto md = scanner->scan_singleton(td.pathname);
        wassert_false(md->has_source());
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
        wassert_false(scan::Scanner::update_sequence_number(mdc[0], usn));
        wassert(actual(usn) == 42);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts.bufr");
        int usn;
        wassert_true(scan::Scanner::update_sequence_number(mdc[0], usn));
        wassert(actual(usn) == 0);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts-usn2.bufr");
        int usn;
        wassert_true(scan::Scanner::update_sequence_number(mdc[0], usn));
        wassert(actual(usn) == 2);
    }
#endif
});

}

}
