#include "arki/types/tests.h"
#include "arki/segment/iseg.h"
#include "session.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_iseg_session");

void Tests::register_tests() {

add_method("segment_from", [] {
    auto session = std::make_shared<arki::segment::iseg::Session>(".");
    auto segment1 = session->segment_from_relpath("foo.grib");
    wassert_true(dynamic_cast<arki::segment::iseg::Segment*>(segment1.get()));
    auto segment2 = session->segment_from_relpath_and_format("foo.grib", DataFormat::GRIB);
    wassert_true(dynamic_cast<arki::segment::iseg::Segment*>(segment2.get()));
});

}

}
