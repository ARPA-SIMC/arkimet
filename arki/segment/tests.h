#ifndef ARKI_SEGMENT_TESTS_H
#define ARKI_SEGMENT_TESTS_H

#include <arki/metadata/tests.h>
#include <arki/segment.h>
#include <arki/metadata/collection.h>

namespace arki::tests {

/**
 * Create a segment with attached metadata.
 *
 * mds metadata sources are updated to point inside the segment
 */
void fill_metadata_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds);

}

#endif
