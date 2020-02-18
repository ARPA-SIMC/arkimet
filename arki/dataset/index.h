#ifndef ARKI_DATASET_INDEX_H
#define ARKI_DATASET_INDEX_H

#include <arki/core/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/dataset/fwd.h>

namespace arki {
struct Summary;

namespace dataset {

struct Index
{
    std::weak_ptr<core::Lock> lock;

    virtual ~Index();

    /**
     * Query this index, returning metadata
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    virtual bool query_data(const dataset::DataQuery& q, metadata_dest_func) = 0;

    /**
     * Query this index, returning a summary
     *
     * Summary caches are used if available.
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    virtual bool query_summary(const Matcher& matcher, Summary& summary) = 0;

    /**
     * Get the lowest and highest reference time for files in the given segment.
     *
     * If the segment does not exist in the index, return false
     */
    virtual bool segment_timespan(const std::string& relpath, core::Time& start_time, core::Time& end_time) const = 0;

    /**
     * List all segments known to the index.
     *
     * Segments are sorted alphabetically by relative paths.
     */
    virtual void list_segments(std::function<void(const std::string&)> dest) = 0;

    /**
     * List all segments known to the index, filtered by a matcher.
     *
     * Segments are sorted alphabetically by relative paths.
     */
    virtual void list_segments_filtered(const Matcher& matcher, std::function<void(const std::string&)> dest) = 0;

    /// Check if a segment is known to the index
    virtual bool has_segment(const std::string& relpath) const = 0;

    /**
     * Get the metadata for a segment.
     */
    virtual void query_segment(const std::string& relpath, metadata_dest_func) const = 0;

    /**
     * Rename a segment in the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     *
     * The version on disk is not touched.
     */
    virtual void test_rename(const std::string& relpath, const std::string& new_relpath) = 0;

    /**
     * Remove a segment from the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     *
     * The version on disk is not touched.
     */
    virtual void test_deindex(const std::string& relpath) = 0;

    /**
     * Update the index so that the offset of all data in the segment starting
     * from the one at position `data_idx` are shifted backwards by
     * `overlap_size`, so that the ones at `data_idx - 1` and at `data_idx`
     * overlap.
     */
    virtual void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx) = 0;

    /**
     * Update the index so that the offset of all data in the segment starting
     * from the one at position `data_idx` are shifted forwards by `hole_size`,
     * so that a hole is created between the ones at `data_idx - 1` and at
     * `data_idx`
     */
    virtual void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx) = 0;
};

}
}
#endif
