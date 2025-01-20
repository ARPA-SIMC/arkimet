#include "tests.h"
#include "data.h"
#include "arki/core/lock.h"
#include "arki/utils/sys.h"

using namespace std::literals;
using namespace arki::utils;

namespace arki::tests {

void fill_scan_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds)
{
    segment::data::WriterConfig wconf{true, true};
    auto data_writer = segment->detect_data()->writer(wconf, false);
    for (auto i: mds)
        data_writer->append(*i);
    data_writer->commit();
}

void fill_metadata_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds)
{
    segment::data::WriterConfig wconf{true, true};
    auto data_writer = segment->detect_data()->writer(wconf, false);
    for (auto i: mds)
        data_writer->append(*i);
    data_writer->commit();
    mds.writeAtomically(sys::with_suffix(segment->abspath(), ".metadata"));
}

void ActualSegment::has_data()
{
    wassert_true((bool)_actual->detect_data()->timestamp());
}

void ActualSegment::not_has_data()
{
    wassert_false((bool)_actual->detect_data()->timestamp());
}

void ActualSegment::has_metadata()
{
    wassert(actual_file(_actual->abspath_metadata()).exists());
}

void ActualSegment::not_has_metadata()
{
    wassert(actual_file(_actual->abspath_metadata()).not_exists());
}

void ActualSegment::has_summary()
{
    wassert(actual_file(_actual->abspath_summary()).exists());
}

void ActualSegment::not_has_summary()
{
    wassert(actual_file(_actual->abspath_summary()).not_exists());
}


void SegmentTest::test_setup()
{
    sys::rmtree_ifexists("test");
    std::filesystem::create_directory("test");
    m_session = make_session("test");
}

std::shared_ptr<Segment> SegmentTest::create(const metadata::Collection& mds, const char* name)
{
    auto segment = m_session->segment_from_relpath("test/"s + name);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    auto fixer = checker->fixer();
    return segment;
}

}
