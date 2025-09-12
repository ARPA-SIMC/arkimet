#ifndef ARKI_SEGMENT_TESTS_H
#define ARKI_SEGMENT_TESTS_H

#include <arki/core/lock.h>
#include <arki/metadata/collection.h>
#include <arki/metadata/tests.h>
#include <arki/segment.h>

namespace arki::tests {

/**
 * Create a bare data segment.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_scan_segment(std::shared_ptr<const Segment> segment,
                       arki::metadata::Collection& mds);

/**
 * Create a segment with attached metadata.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_metadata_segment(std::shared_ptr<const Segment> segment,
                           arki::metadata::Collection& mds);

/**
 * Create a segment with attached iseg index.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_iseg_segment(std::shared_ptr<const Segment> segment,
                       arki::metadata::Collection& mds);

struct ActualSegment
    : public arki::utils::tests::Actual<std::shared_ptr<const Segment>>
{
    explicit ActualSegment(std::shared_ptr<const Segment> s)
        : Actual<std::shared_ptr<const Segment>>(s)
    {
    }

    /// The segment has its data part
    void has_data();

    /// The segment does not have its data part
    void not_has_data();

    /// The segment has its data part
    void has_metadata();

    /// The segment does not have its data part
    void not_has_metadata();

    /// The segment has its data part
    void has_summary();

    /// The segment does not have its data part
    void not_has_summary();

    /// The segment data mtime is exactly value
    void data_mtime_equal(time_t value);

    /// The segment data mtime is newer than value
    void data_mtime_newer_than(time_t value);

    /// Import a metadata into the segment
    arki::segment::Writer::AcquireResult
    imports(std::shared_ptr<arki::Metadata> md,
            const segment::WriterConfig& config = segment::WriterConfig(),
            std::shared_ptr<core::AppendLock> lock =
                std::make_shared<core::lock::NullAppendLock>());
};

inline arki::tests::ActualSegment actual(std::shared_ptr<Segment> actual)
{
    return arki::tests::ActualSegment(actual);
}
inline arki::tests::ActualSegment actual(std::shared_ptr<const Segment> actual)
{
    return arki::tests::ActualSegment(actual);
}
inline arki::tests::ActualSegment actual(const Segment& actual)
{
    return arki::tests::ActualSegment(actual.shared_from_this());
}

template <class Data> class SegmentTestFixture : public Fixture
{
protected:
    std::shared_ptr<segment::Session> m_session;

public:
    Data td;
    time_t initial_mtime = 1234000;

    SegmentTestFixture() = default;
    virtual ~SegmentTestFixture() {}

    void test_setup();

    /// Create a new segmeng session for the given path
    virtual std::shared_ptr<segment::Session>
    make_session(const std::filesystem::path& root) = 0;

    std::shared_ptr<Segment> create_segment(const char* name = "segment");
    std::shared_ptr<Segment> create(const char* name = "segment");
    virtual std::shared_ptr<Segment> create(const metadata::Collection& mds,
                                            const char* name = "segment") = 0;

    virtual void skip_unless_scan() const
    {
        throw TestSkipped("test is only valid for scan segments");
    }
    virtual void skip_unless_metadata() const
    {
        throw TestSkipped("test is only valid for metadata segments");
    }
    virtual void skip_unless_iseg() const
    {
        throw TestSkipped("test is only valid for iseg segments");
    }
    virtual void skip_unless_has_index() const
    {
        throw TestSkipped("test is only valid for segments with indices");
    }

    virtual bool has_index() const { return false; }
};

template <class Data> class ScanSegmentFixture : public SegmentTestFixture<Data>
{
protected:
    using SegmentTestFixture<Data>::m_session;

public:
    using SegmentTestFixture<Data>::td;
    using SegmentTestFixture<Data>::create_segment;
    using SegmentTestFixture<Data>::initial_mtime;

    std::shared_ptr<segment::Session>
    make_session(const std::filesystem::path& root) override;

    std::shared_ptr<Segment> create(const metadata::Collection& mds,
                                    const char* name = "segment") override;

    void skip_unless_scan() const override {}
};

template <class Data>
class MetadataSegmentFixture : public SegmentTestFixture<Data>
{
protected:
    using SegmentTestFixture<Data>::m_session;

public:
    using SegmentTestFixture<Data>::td;
    using SegmentTestFixture<Data>::create_segment;
    using SegmentTestFixture<Data>::initial_mtime;

    std::shared_ptr<segment::Session>
    make_session(const std::filesystem::path& root) override;

    std::shared_ptr<Segment> create(const metadata::Collection& mds,
                                    const char* name = "segment") override;

    void skip_unless_metadata() const override {}
    void skip_unless_has_index() const override {}
    bool has_index() const override { return true; }
};

template <class Data> class IsegSegmentFixture : public SegmentTestFixture<Data>
{
protected:
    using SegmentTestFixture<Data>::m_session;

public:
    using SegmentTestFixture<Data>::td;
    using SegmentTestFixture<Data>::create_segment;
    using SegmentTestFixture<Data>::initial_mtime;

    std::shared_ptr<segment::Session>
    make_session(const std::filesystem::path& root) override;

    std::shared_ptr<Segment> create(const metadata::Collection& mds,
                                    const char* name = "segment") override;

    void skip_unless_iseg() const override {}
    void skip_unless_has_index() const override {}
    bool has_index() const override { return true; }
};

} // namespace arki::tests

#endif
