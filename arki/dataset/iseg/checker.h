#ifndef ARKI_DATASET_ISEG_CHECKER_H
#define ARKI_DATASET_ISEG_CHECKER_H

#include <arki/dataset/iseg.h>
#include <string>

namespace arki {
class Matcher;

namespace dataset {
namespace iseg {
class WIndex;
class CheckerSegment;

class Checker : public segmented::Checker
{
protected:
    std::shared_ptr<const iseg::Dataset> m_config;

    void list_segments(std::function<void(const std::string& relpath)> dest);
    void list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest);

public:
    Checker(std::shared_ptr<const iseg::Dataset> config);

    const iseg::Dataset& config() const override { return *m_config; }

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments_tracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void check_issue51(CheckerConfig& opts) override;
    size_t vacuum(dataset::Reporter& reporter) override;

    void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx=1) override;
    void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx=0) override;
    void test_corrupt_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_truncate_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx) override;
    void test_rename(const std::string& relpath, const std::string& new_relpath) override;
    void test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx) override;
    void test_delete_from_index(const std::string& relpath) override;
    void test_invalidate_in_index(const std::string& relpath) override;

    friend class CheckerSegment;
};

}
}
}
#endif

