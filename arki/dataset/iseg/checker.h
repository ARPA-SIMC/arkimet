#ifndef ARKI_DATASET_ISEG_CHECKER_H
#define ARKI_DATASET_ISEG_CHECKER_H

#include <arki/dataset/impl.h>
#include <arki/dataset/iseg.h>
#include <filesystem>
#include <string>

namespace arki {
class Matcher;

namespace dataset {
namespace iseg {

class Checker : public DatasetAccess<iseg::Dataset, segmented::Checker>
{
protected:
    void
    list_segments(std::function<void(std::shared_ptr<const Segment>)> dest);
    void
    list_segments(const Matcher& matcher,
                  std::function<void(std::shared_ptr<const Segment>)> dest);

public:
    explicit Checker(std::shared_ptr<iseg::Dataset> dataset);

    std::string type() const override;

    void remove(const metadata::Collection& mds) override;
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
    void check_issue51(CheckerConfig& opts) override;
    size_t vacuum(dataset::Reporter& reporter) override;

    void
    test_invalidate_in_index(const std::filesystem::path& relpath) override;

    friend class CheckerSegment;
};

} // namespace iseg
} // namespace dataset
} // namespace arki
#endif
