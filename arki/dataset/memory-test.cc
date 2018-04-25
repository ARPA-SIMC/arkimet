#include "arki/types/tests.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/segment.h"
#include "arki/utils/sys.h"
#include "memory.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_memory");

void Tests::register_tests() {

// Test querying
add_method("query", [] {
    auto reader = Segment::detect_reader("grib", sys::abspath("."), "inbound/test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());
    dataset::Memory c;
    reader->scan(c.inserter_func());

    metadata::Collection mdc(c, Matcher::parse("origin:GRIB1,200"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));

    mdc.clear();

    mdc.add(c, Matcher::parse("origin:GRIB1,80"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 7218, 34960u));

    mdc.clear();
    mdc.add(c, Matcher::parse("origin:GRIB1,98"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 42178, 2234));
});

// Test querying the summary
add_method("query_summary", [] {
    auto reader = Segment::detect_reader("grib", sys::abspath("."), "inbound/test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());
    dataset::Memory c;
    reader->scan(c.inserter_func());

    Summary summary;
    c.query_summary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
});

// Test querying the summary by reftime
add_method("query_summary_reftime", [] {
    auto reader = Segment::detect_reader("grib", sys::abspath("."), "inbound/test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());
    dataset::Memory c;
    reader->scan(c.inserter_func());

    Summary summary;
    //system("bash");
    c.query_summary(Matcher::parse("reftime:>=2007-07"), summary);
    ensure_equals(summary.count(), 3u);
});

}

}
