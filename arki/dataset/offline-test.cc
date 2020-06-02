#include "arki/tests/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset/session.h"
#include "arki/dataset/query.h"
#include "offline.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using arki::core::Time;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_offline");

void Tests::register_tests() {

add_method("read", []() {
    metadata::TestCollection mdc("inbound/test.grib1");
    Summary sum;
    mdc.add_to_summary(sum);
    sum.writeAtomically("test-offline.summary");

    auto session = std::make_shared<dataset::Session>();
    auto config = std::make_shared<dataset::offline::Dataset>(session, "test-offline");
    auto reader = std::make_shared<dataset::offline::Reader>(config);
    size_t count = 0;
    reader->query_data(Matcher(), [&](std::shared_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 0u);

    Summary sum1;
    reader->query_summary(Matcher(), sum1);
    wassert(actual(sum == sum1));

    core::Interval interval = reader->get_stored_time_interval();
    wassert(actual(interval.begin) == "2007-07-07T00:00:00Z");
    wassert(actual(interval.end) == "2007-10-09T00:00:01Z");
});

}

}
