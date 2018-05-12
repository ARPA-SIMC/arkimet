#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/utils/sys.h"
#include "dir.h"

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
} test("arki_scan_dir");

void Tests::register_tests() {

// Scan a well-known vm2 sample
add_method("scan", []() {
    skip_unless_odimh5();
    Metadata md;
    scan::Dir scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/fixture.odimh5", mds.inserter_func());
    wassert(actual(mds.size()) == 3u);
    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/fixture.odimh5", 0, 49057));
});

}
}
