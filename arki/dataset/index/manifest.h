#ifndef ARKI_DATASET_INDEX_MANIFEST_H
#define ARKI_DATASET_INDEX_MANIFEST_H

#include <arki/dataset/index.h>
#include <arki/core/transaction.h>
#include <vector>
#include <string>
#include <memory>

namespace arki {
class Matcher;
class Summary;

namespace dataset {
namespace index {

class Manifest : public dataset::Index
{
protected:
    std::string m_path;
    const core::lock::Policy* lock_policy;
    void querySummaries(const Matcher& matcher, Summary& summary);

public:
    Manifest(const std::string& path, const core::lock::Policy* lock_policy);
    virtual ~Manifest();

    virtual void openRO() = 0;
    virtual void openRW() = 0;

    /// Return the list of segments, sorted by the start reftime of their contents
    virtual std::vector<std::string> file_list(const Matcher& matcher) = 0;
    bool segment_timespan(const std::string& relpath, core::Time& start_time, core::Time& end_time) const override = 0;
    virtual size_t vacuum() = 0;
    virtual void acquire(const std::string& relpath, time_t mtime, const Summary& sum) = 0;
    virtual void remove(const std::string& relpath) = 0;
    virtual void flush() = 0;

    virtual core::Pending test_writelock() = 0;

    /// Invalidate global summary
    void invalidate_summary();
    /// Invalidate summary for file \a relpath and global summary
    void invalidate_summary(const std::string& relpath);

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this manifest.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * manifest.
     */
    virtual void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const = 0;

    bool query_data(const dataset::DataQuery& q, dataset::Session& session, metadata_dest_func) override;
    bool query_summary(const Matcher& matcher, Summary& summary) override;
    void query_segment(const std::string& relpath, dataset::Session& session, metadata_dest_func) const override;
    void list_segments(std::function<void(const std::string&)> dest) override = 0;
    void list_segments_filtered(const Matcher& matcher, std::function<void(const std::string&)> dest) override = 0;
    virtual time_t segment_mtime(const std::string& relpath) const = 0;

    /// Check if the given directory contains a manifest file
    static bool exists(const std::string& dir);
    static std::unique_ptr<Manifest> create(const std::string& dir, const core::lock::Policy* lock_policy, const std::string& index_type=std::string());

    static bool get_force_sqlite();
    static void set_force_sqlite(bool val);

    void test_deindex(const std::string& relpath) override;
    void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx) override;
    void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx) override;
};

}
}
}

#endif
