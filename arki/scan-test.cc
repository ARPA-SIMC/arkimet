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

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_any");

void Tests::register_tests() {

// Test format_from_ext
add_method("format_from_filename", [] {
    wassert(actual(scan::Scanner::format_from_filename("test.grib")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib1")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.grib2")) == "grib");
    wassert(actual(scan::Scanner::format_from_filename("test.bufr")) == "bufr");
#ifdef HAVE_HDF5
    wassert(actual(scan::Scanner::format_from_filename("test.h5")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.hdf5")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odim")) == "odimh5");
    wassert(actual(scan::Scanner::format_from_filename("test.odimh5")) == "odimh5");
#endif
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.foo"));
    wassert_throws(std::runtime_error, scan::Scanner::format_from_filename("test.grib.tar"));
});

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
