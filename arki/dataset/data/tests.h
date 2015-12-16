#ifndef ARKI_DATASET_DATA_TESTS_H
#define ARKI_DATASET_DATA_TESTS_H

#include <arki/dataset/tests.h>
#include <arki/dataset/data.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <string>

namespace arki {
struct Metadata;
struct Dispatcher;
struct Reader;
struct Writer;

namespace tests {

struct SegmentTest
{
    std::string format;
    std::string relname;
    std::string absname;
    metadata::Collection mdc;

    SegmentTest();
    virtual ~SegmentTest();

    /// Instantiate the segment to use for testing
    virtual dataset::data::Segment* make_segment() = 0;

    virtual void run() = 0;

    /// Create a segment with no data on disk
    std::unique_ptr<dataset::data::Segment> make_empty_segment();

    /// Create a segment importing all mdc into it
    std::unique_ptr<dataset::data::Segment> make_full_segment();

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

// TODO: virtual void truncate(const std::string& absname, size_t offset) = 0;
// TODO: virtual Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds) = 0;

}
}

#endif
