#ifndef ARKI_SEGMENT_TESTS_H
#define ARKI_SEGMENT_TESTS_H

#include <arki/metadata/tests.h>
#include <arki/segment.h>
#include <arki/metadata/collection.h>

namespace arki::tests {

/**
 * Create a bare data segment.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_scan_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds);

/**
 * Create a segment with attached metadata.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_metadata_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds);

struct ActualSegment : public arki::utils::tests::Actual<std::shared_ptr<const Segment>>
{
    explicit ActualSegment(std::shared_ptr<const Segment> s) : Actual<std::shared_ptr<const Segment>>(s) {}

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
};

inline arki::tests::ActualSegment actual(std::shared_ptr<const Segment> actual) { return arki::tests::ActualSegment(actual); }
inline arki::tests::ActualSegment actual(const Segment& actual) { return arki::tests::ActualSegment(actual.shared_from_this()); }

}

#endif
