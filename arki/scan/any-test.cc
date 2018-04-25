#include "arki/metadata/tests.h"
#include "arki/libconfig.h"
#include "arki/scan/any.h"
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

// Test reading update sequence numbers
add_method("usn", [] {
#ifdef HAVE_DBALLE
    {
        // Gribs don't have update sequence numbrs, and the usn parameter must
        // be left untouched
        metadata::TestCollection mdc("inbound/test.grib1");
        int usn = 42;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), false);
        ensure_equals(usn, 42);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts.bufr");
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 0);
    }

    {
        metadata::TestCollection mdc("inbound/synop-gts-usn2.bufr");
        int usn;
        ensure_equals(scan::update_sequence_number(mdc[0], usn), true);
        ensure_equals(usn, 2);
    }
#endif
});

}

}
