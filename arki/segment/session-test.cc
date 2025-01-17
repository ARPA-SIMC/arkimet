#include "arki/segment/tests.h"
#include "arki/core/lock.h"
#include "arki/utils/sys.h"
#include "session.h"
#include "metadata.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;

    void test_setup() override
    {
        sys::rmtree_ifexists("test");
        std::filesystem::create_directories("test");
    }
} test("arki_segment_session");

std::shared_ptr<Segment> make_segment(TestData& td, const char* name="test/test")
{
    auto session = std::make_shared<segment::Session>(".");
    return session->segment_from_relpath(sys::with_suffix(name, "." + format_name(td.format)));

}

void Tests::register_tests() {

add_method("detect_metadata_reader", [] {
    GRIBData td;
    auto segment = make_segment(td);
    fill_metadata_segment(segment, td.mds);
    wassert(actual_file("test/test.grib").exists());
    wassert(actual_file("test/test.grib.metadata").exists());

    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    wassert_true(dynamic_cast<segment::metadata::Reader*>(reader.get()));

    auto e = wassert_throws(std::runtime_error, segment->checker(std::make_shared<core::lock::NullCheckLock>()));
    wassert(actual(e.what()).contains("misses a policy"));
});


}

}

