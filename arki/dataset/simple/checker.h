#ifndef ARKI_DATASET_SIMPLE_CHECKER_H
#define ARKI_DATASET_SIMPLE_CHECKER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/impl.h>
#include <string>
#include <iosfwd>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}

namespace simple {
class CheckerSegment;

class Checker : public DatasetAccess<simple::Dataset, indexed::Checker>
{
protected:
    index::Manifest* m_mft;
    std::shared_ptr<dataset::CheckLock> lock;

    /// Return a (shared) instance of the Segment for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Checker();

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments_tracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void repack(CheckerConfig& opts, unsigned test_flags=0) override;
    void check(CheckerConfig& opts) override;

    size_t vacuum(dataset::Reporter& reporter) override;
    void test_delete_from_index(const std::string& relpath) override;
    void test_invalidate_in_index(const std::string& relpath) override;
    std::shared_ptr<Metadata> test_change_metadata(const std::string& relpath, std::shared_ptr<Metadata> md, unsigned data_idx) override;

    friend class CheckerSegment;
};

}
}
}
#endif

