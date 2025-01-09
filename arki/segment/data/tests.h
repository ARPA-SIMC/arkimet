#ifndef ARKI_SEGMENT_TESTS_H
#define ARKI_SEGMENT_TESTS_H

#include <arki/metadata/tests.h>
#include <arki/segment/data.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <string>

namespace arki {
namespace tests {

template<class Data, class FixtureData>
struct SegmentFixture : public Fixture
{
    FixtureData td;
    segment::data::RepackConfig repack_config;
    metadata::Collection seg_mds;
    std::shared_ptr<Segment> segment;

    std::shared_ptr<segment::data::Checker> create();
    std::shared_ptr<segment::data::Checker> create(metadata::Collection mds);
    std::shared_ptr<segment::data::Checker> create(const segment::data::RepackConfig& cfg);

    SegmentFixture(const segment::data::RepackConfig& cfg=segment::data::RepackConfig())
        : repack_config(cfg) {}

    void test_setup();
};

template<class Data, class FixtureData>
struct SegmentTests : public FixtureTestCase<SegmentFixture<Data, FixtureData>>
{
    typedef SegmentFixture<Data, FixtureData> Fixture;
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
};

struct SegmentTest
{
    std::string format;
    std::string root;
    std::string relpath;
    std::string abspath;
    metadata::TestCollection mdc;

    SegmentTest();
    virtual ~SegmentTest();

    /// Instantiate the segment to use for testing
    virtual std::shared_ptr<segment::data::Writer> make_writer() = 0;
    virtual std::shared_ptr<segment::data::Checker> make_checker() = 0;

    virtual void run() = 0;

    /// Create a segment with no data on disk
    std::shared_ptr<segment::data::Writer> make_empty_writer();
    std::shared_ptr<segment::data::Checker> make_empty_checker();

    /// Create a segment importing all mdc into it
    std::shared_ptr<segment::data::Writer> make_full_writer();
    virtual std::shared_ptr<segment::data::Checker> make_full_checker();

    void append_all();
};

/**
 * Test Segment::check function
 */
struct SegmentCheckTest : public SegmentTest
{
    void run() override;
};

/**
 * Test Segment::remove function
 */
struct SegmentRemoveTest : public SegmentTest
{
    void run() override;
};

// TODO: virtual void truncate(const std::string& abspath, size_t offset) = 0;
// TODO: virtual Pending repack(const std::string& rootdir, const std::string& relpath, metadata::Collection& mds) = 0;

void test_append_transaction_ok(segment::data::Writer* dw, Metadata& md, int append_amount_adjust=0);
void test_append_transaction_rollback(segment::data::Writer* dw, Metadata& md, int append_amount_adjust=0);

struct ActualSegment: public arki::utils::tests::Actual<std::string>
{
    using Actual<std::string>::Actual;

    void exists();
    void exists(const std::vector<std::string>& extensions);
    void not_exists();
};

inline ActualSegment actual_segment(const std::string& path) { return ActualSegment(path); }

}
}
#endif
