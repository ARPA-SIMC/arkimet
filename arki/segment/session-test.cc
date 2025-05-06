#include "arki/segment/tests.h"
#include "arki/core/lock.h"
#include "arki/utils/sys.h"
#include "session.h"
#include "metadata.h"
#include "scan.h"
#include <filesystem>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    std::shared_ptr<segment::Session> session;

    void register_tests() override;

    void test_setup() override
    {
        sys::rmtree_ifexists("test");
        std::filesystem::create_directories("test");
        session = std::make_shared<segment::Session>("test");
    }
} test("arki_segment_session");

void Tests::register_tests() {

add_method("segment_from_relpath", [&] {
    std::shared_ptr<Segment> segment;

    wassert(segment = session->segment_from_relpath("foo.grib"));
    wassert(actual(segment->abspath()) == session->root / "foo.grib");
    wassert(actual(segment->relpath()) == "foo.grib");

    wassert(segment = session->segment_from_relpath("../test/foo.grib"));
    wassert(actual(segment->abspath()) == session->root / "foo.grib");
    wassert(actual(segment->relpath()) == "foo.grib");

    auto e = wassert_throws(std::runtime_error, session->segment_from_relpath("../foo.grib"));
    wassert(actual(e.what()) == "relative segment path points outside the segment root");

    e = wassert_throws(std::runtime_error, session->segment_from_relpath_and_format("", DataFormat::GRIB));
    wassert(actual(e.what()) == "relative segment path is empty");
});

add_method("detect_metadata", [&] {
    GRIBData td;
    auto segment = session->segment_from_relpath_and_format("test." + format_name(td.format), td.format);
    session->create_metadata(segment, td.mds);
    wassert(actual_file(segment->abspath()).exists());
    wassert(actual_file(segment->abspath_metadata()).exists());
    wassert(actual_file(segment->abspath_iseg_index()).not_exists());

    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    wassert_true(dynamic_cast<segment::metadata::Reader*>(reader.get()));

    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    wassert_true(dynamic_cast<segment::metadata::Checker*>(checker.get()));

    // If there is only a .metadata file, we still get a metadata checker
    std::filesystem::remove(segment->abspath());
    checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    wassert_true(dynamic_cast<segment::metadata::Checker*>(checker.get()));
});

add_method("detect_scan", [&] {
    GRIBData td;
    auto segment = session->segment_from_relpath_and_format("test." + format_name(td.format), td.format);
    session->create_scan(segment, td.mds);
    wassert(actual_file(segment->abspath()).exists());
    wassert(actual_file(segment->abspath_metadata()).not_exists());
    wassert(actual_file(segment->abspath_iseg_index()).not_exists());

    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    wassert_true(dynamic_cast<segment::scan::Reader*>(reader.get()));

    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    wassert_true(dynamic_cast<segment::scan::Checker*>(checker.get()));
});

add_method("detect_new", [&] {
    auto segment = session->segment_from_relpath("test.grib");
    wassert(actual_file(segment->abspath()).not_exists());
    wassert(actual_file(segment->abspath_metadata()).not_exists());
    wassert(actual_file(segment->abspath_iseg_index()).not_exists());

    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    wassert_true(dynamic_cast<segment::scan::Reader*>(reader.get()));

    auto e = wassert_throws(std::runtime_error, segment->checker(std::make_shared<core::lock::NullCheckLock>()));
    wassert(actual(e.what()).contains("misses a policy"));
});

}

}

