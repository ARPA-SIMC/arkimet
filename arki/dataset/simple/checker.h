#ifndef ARKI_DATASET_SIMPLE_CHECKER_H
#define ARKI_DATASET_SIMPLE_CHECKER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/impl.h>
#include <arki/dataset/simple.h>
#include <arki/dataset/simple/manifest.h>
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
    manifest::Writer manifest;
    std::shared_ptr<core::CheckLock> lock;

public:
    explicit Checker(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Checker();

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment>
    segment(std::shared_ptr<const Segment> segment) override;
    std::unique_ptr<segmented::CheckerSegment>
    segment_prelocked(std::shared_ptr<const Segment> segment,
                      std::shared_ptr<core::CheckLock> lock) override;
    void segments_tracked(
        std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_tracked_filtered(
        const Matcher& matcher,
        std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(
        std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(
        const Matcher& matcher,
        std::function<void(segmented::CheckerSegment& segment)>) override;
    void repack(CheckerConfig& opts, unsigned test_flags = 0) override;
    void check(CheckerConfig& opts) override;

    size_t vacuum(dataset::Reporter& reporter) override;
    void test_delete_from_index(const std::filesystem::path& relpath) override;
    void
    test_invalidate_in_index(const std::filesystem::path& relpath) override;
    metadata::Collection
    test_change_metadata(const std::filesystem::path& relpath,
                         std::shared_ptr<Metadata> md,
                         unsigned data_idx) override;

    void check_issue51(CheckerConfig& opts) override;

    void test_rename(const std::filesystem::path& relpath,
                     const std::filesystem::path& new_relpath) override;
    void test_touch_contents(time_t timestamp) override;

    friend class CheckerSegment;
};

} // namespace simple
} // namespace dataset
} // namespace arki
#endif
