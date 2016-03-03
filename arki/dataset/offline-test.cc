#include "arki/tests/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "offline.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_offline");

void Tests::register_tests() {

add_method("read", []() {
    metadata::Collection mdc("inbound/test.grib1");
    Summary sum;
    mdc.add_to_summary(sum);
    sum.writeAtomically("test-offline.summary");

    dataset::OfflineReader reader("test-offline.summary");
    size_t count = 0;
    reader.query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 0u);

    Summary sum1;
    reader.query_summary(Matcher(), sum1);
    wassert(actual(sum == sum1));

    unique_ptr<types::Time> begin, end;
    reader.expand_date_range(begin, end);
    wassert(actual(*begin) == "2007-07-07T00:00:00Z");
    wassert(actual(*end) == "2007-10-09T00:00:00Z");
});

}

}