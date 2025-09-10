#include "tests.h"
#include "arki/core/lock.h"
#include "arki/segment/iseg.h"
#include "arki/segment/iseg/session.h"
#include "arki/utils/sys.h"
#include "data.h"

using namespace std::literals;
using namespace arki::utils;

namespace arki::tests {

void fill_scan_segment(std::shared_ptr<const Segment> segment,
                       arki::metadata::Collection& mds)
{
    segment::WriterConfig wconf{"test"};
    wconf.drop_cached_data_on_commit = true;
    auto data                        = segment::Data::create(segment);
    auto data_writer                 = data->writer(wconf);
    for (auto i : mds)
        data_writer->append(*i);
    data_writer->commit();
}

void fill_metadata_segment(std::shared_ptr<const Segment> segment,
                           arki::metadata::Collection& mds)
{
    segment::WriterConfig wconf{"test"};
    wconf.drop_cached_data_on_commit = true;
    auto data                        = segment::Data::create(segment);
    auto data_writer                 = data->writer(wconf);
    for (auto i : mds)
        data_writer->append(*i);
    data_writer->commit();
    mds.writeAtomically(sys::with_suffix(segment->abspath(), ".metadata"));
}

void ActualSegment::has_data()
{
    wassert_true(
        (bool)_actual->checker(std::make_shared<core::lock::NullCheckLock>())
            ->timestamp());
}

void ActualSegment::not_has_data()
{
    wassert_false(
        (bool)_actual->checker(std::make_shared<core::lock::NullCheckLock>())
            ->timestamp());
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

void ActualSegment::data_mtime_equal(time_t value)
{
    auto ts = _actual->checker(std::make_shared<core::lock::NullCheckLock>())
                  ->timestamp();
    if (!ts)
    {
        std::stringstream buf;
        buf << "data of " << _actual->relpath() << " should have mtime "
            << value << " but it does not exist";
        throw TestFailed(buf.str());
    }

    if (ts.value() != value)
    {
        std::stringstream buf;
        buf << "data of " << _actual->relpath() << " has mtime " << ts.value()
            << " instead of " << value;
        throw TestFailed(buf.str());
    }
}

void ActualSegment::data_mtime_newer_than(time_t value)
{
    auto ts = _actual->checker(std::make_shared<core::lock::NullCheckLock>())
                  ->timestamp();
    if (!ts)
    {
        std::stringstream ss;
        ss << "data of " << _actual->relpath()
           << " should have mtime newer than " << value
           << " but it does not exist";
        throw TestFailed(ss.str());
    }

    if (ts.value() <= value)
    {
        std::stringstream ss;
        ss << "data of " << _actual->relpath() << " has mtime " << ts.value()
           << " which is not newer than " << value;
        throw TestFailed(ss.str());
    }
}

template <typename Data> void SegmentTestFixture<Data>::test_setup()
{
    sys::rmtree_ifexists("test");
    std::filesystem::create_directory("test");
    m_session = make_session("test");
}

template <typename Data>
std::shared_ptr<Segment>
SegmentTestFixture<Data>::create_segment(const char* name)
{
    auto segment = m_session->segment_from_relpath(
        sys::with_suffix(name, "." + format_name(td.format)));
    std::filesystem::create_directories(segment->abspath().parent_path());
    return segment;
}

template <typename Data>
std::shared_ptr<Segment> SegmentTestFixture<Data>::create(const char* name)
{
    return create(td.mds, name);
}

template <typename Data>
std::shared_ptr<segment::Session>
ScanSegmentFixture<Data>::make_session(const std::filesystem::path& root)
{
    return std::make_shared<segment::ScanSession>(root);
}

template <typename Data>
std::shared_ptr<Segment>
ScanSegmentFixture<Data>::create(const metadata::Collection& mds,
                                 const char* name)
{
    auto segment = create_segment(name);
    auto mds1    = mds.clone();
    m_session->create_scan(segment, mds1);

    auto checker =
        segment->checker(std::make_shared<core::lock::NullCheckLock>());
    checker->fixer()->test_touch_contents(initial_mtime);

    return segment;
}

template <typename Data>
std::shared_ptr<segment::Session>
MetadataSegmentFixture<Data>::make_session(const std::filesystem::path& root)
{
    return std::make_shared<segment::MetadataSession>(root);
}

template <typename Data>
std::shared_ptr<Segment>
MetadataSegmentFixture<Data>::create(const metadata::Collection& mds,
                                     const char* name)
{
    auto segment = create_segment(name);
    auto mds1    = mds.clone();
    m_session->create_metadata(segment, mds1);

    auto checker =
        segment->checker(std::make_shared<core::lock::NullCheckLock>());
    checker->fixer()->test_touch_contents(initial_mtime);

    return segment;
}

template <typename Data>
std::shared_ptr<segment::Session>
IsegSegmentFixture<Data>::make_session(const std::filesystem::path& root)
{
    auto res    = std::make_shared<segment::iseg::Session>(root);
    res->format = td.format;
    res->index.emplace(TYPE_ORIGIN);
    res->index.emplace(TYPE_REFTIME);
    res->unique.emplace(TYPE_PRODUCT);
    res->unique.emplace(TYPE_AREA);
    res->unique.emplace(TYPE_REFTIME);
    res->smallfiles = false;
    res->eatmydata  = true;
    return res;
}

template <typename Data>
std::shared_ptr<Segment>
IsegSegmentFixture<Data>::create(const metadata::Collection& mds,
                                 const char* name)
{
    auto segment = create_segment(name);
    auto mds1    = mds.clone();
    m_session->create_iseg(segment, mds1);

    auto checker =
        segment->checker(std::make_shared<core::lock::NullCheckLock>());
    checker->fixer()->test_touch_contents(initial_mtime);

    return segment;
}

template class SegmentTestFixture<GRIBData>;
template class SegmentTestFixture<BUFRData>;
template class SegmentTestFixture<VM2Data>;
template class SegmentTestFixture<ODIMData>;
template class SegmentTestFixture<NCData>;
template class SegmentTestFixture<JPEGData>;

template class ScanSegmentFixture<GRIBData>;
template class ScanSegmentFixture<BUFRData>;
template class ScanSegmentFixture<VM2Data>;
template class ScanSegmentFixture<ODIMData>;
template class ScanSegmentFixture<NCData>;
template class ScanSegmentFixture<JPEGData>;

template class MetadataSegmentFixture<GRIBData>;
template class MetadataSegmentFixture<BUFRData>;
template class MetadataSegmentFixture<VM2Data>;
template class MetadataSegmentFixture<ODIMData>;
template class MetadataSegmentFixture<NCData>;
template class MetadataSegmentFixture<JPEGData>;

template class IsegSegmentFixture<GRIBData>;
template class IsegSegmentFixture<BUFRData>;
template class IsegSegmentFixture<VM2Data>;
template class IsegSegmentFixture<ODIMData>;
template class IsegSegmentFixture<NCData>;
template class IsegSegmentFixture<JPEGData>;

} // namespace arki::tests
