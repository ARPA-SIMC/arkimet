#ifndef ARKI_DATASET_INDEX_H
#define ARKI_DATASET_INDEX_H

#include <arki/dataset.h>
#include <arki/dataset/segment.h>

namespace arki {
namespace dataset {

struct Index
{
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
    virtual bool segment_timespan(const std::string& relname, core::Time& start_time, core::Time& end_time) const = 0;

    /**
     * List all segments known to the index.
     *
     * Segments are sorted alphabetically by relative paths.
     */
    virtual void list_segments(std::function<void(const std::string&)> dest) = 0;

    /**
     * Generate all segment info found in the index.
     *
     * Segments are sorted alphabetically by relative paths.
     *
     * Metadata in the collection are sorted by (reftime, offset).
     */
    virtual void scan_files(segment::contents_func v) = 0;

    /**
     * Rename a segment in the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     *
     * The version on disk is not touched.
     */
    virtual void test_rename(const std::string& relname, const std::string& new_relname) = 0;

    /**
     * Remove a segment from the index.
     *
     * This is used to simulate anomalies in the dataset during tests.
     *
     * The version on disk is not touched.
     */
    virtual void test_remove(const std::string& relname) = 0;
};

}
}
#endif
