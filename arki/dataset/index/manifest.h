#ifndef ARKI_DATASET_INDEX_MANIFEST_H
#define ARKI_DATASET_INDEX_MANIFEST_H

#include <arki/dataset/index.h>
#include <arki/core/transaction.h>
#include <vector>
#include <string>
#include <memory>

namespace arki {
namespace dataset {
namespace simple {
struct Dataset;
}

namespace index {

class Manifest : public dataset::Index
{
protected:
    std::shared_ptr<simple::Dataset> dataset;
    std::filesystem::path m_path;
    const core::lock::Policy* lock_policy;
    void querySummaries(const Matcher& matcher, Summary& summary);

public:
    Manifest(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Manifest();

    virtual void openRO() = 0;
    virtual void openRW() = 0;

    /// Return the list of segments, sorted by the start reftime of their contents
    virtual std::vector<std::filesystem::path> file_list(const Matcher& matcher) = 0;
    bool segment_timespan(const std::filesystem::path& relpath, core::Interval& interval) const override = 0;
    virtual size_t vacuum() = 0;
    virtual void acquire(const std::filesystem::path& relpath, time_t mtime, const Summary& sum) = 0;
    virtual void remove(const std::filesystem::path& relpath) = 0;
    virtual void flush() = 0;

    virtual core::Pending test_writelock() = 0;

    /// Invalidate global summary
    void invalidate_summary();
    /// Invalidate summary for file \a relpath and global summary
    void invalidate_summary(const std::filesystem::path& relpath);

    virtual core::Interval get_stored_time_interval() const = 0;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    bool query_summary(const Matcher& matcher, Summary& summary) override;
    void query_segment(const std::filesystem::path& relpath, metadata_dest_func) const override;
    void list_segments(std::function<void(const std::filesystem::path&)> dest) override = 0;
    void list_segments_filtered(const Matcher& matcher, std::function<void(const std::filesystem::path&)> dest) override = 0;
    virtual time_t segment_mtime(const std::filesystem::path& relpath) const = 0;

    /// Check if the given directory contains a manifest file
    static bool exists(const std::filesystem::path& dir);
    static std::unique_ptr<Manifest> create(std::shared_ptr<simple::Dataset> dataset, const std::string& index_type=std::string());

    static bool get_force_sqlite();
    static void set_force_sqlite(bool val);

    void test_deindex(const std::filesystem::path& relpath) override;
    void test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx) override;
    void test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx) override;
};

}
}
}

#endif
