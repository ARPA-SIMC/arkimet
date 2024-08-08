#ifndef ARKI_DATASET_SIMPLE_CHECKER_H
#define ARKI_DATASET_SIMPLE_CHECKER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}

namespace simple {

class Checker : public DatasetAccess<simple::Dataset, segmented::Checker>
{
protected:
    Index* m_idx = nullptr;
    index::Manifest* m_mft;
    std::shared_ptr<dataset::CheckLock> lock;

    /// Return a (shared) instance of the Segment for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Checker();

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::filesystem::path& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::filesystem::path& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments_tracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void repack(CheckerConfig& opts, unsigned test_flags=0) override;
    void check(CheckerConfig& opts) override;

    size_t vacuum(dataset::Reporter& reporter) override;
    void test_delete_from_index(const std::filesystem::path& relpath) override;
    void test_invalidate_in_index(const std::filesystem::path& relpath) override;
    std::shared_ptr<Metadata> test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx) override;

    void check_issue51(CheckerConfig& opts) override;

    void test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx=1) override;
    void test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx=0) override;
    void test_corrupt_data(const std::filesystem::path& relpath, unsigned data_idx=0) override;
    void test_truncate_data(const std::filesystem::path& relpath, unsigned data_idx=0) override;
    void test_swap_data(const std::filesystem::path& relpath, unsigned d1_idx, unsigned d2_idx) override;
    void test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath) override;

    friend class CheckerSegment;
};

}
}
}
#endif

