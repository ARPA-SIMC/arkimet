#ifndef ARKI_DATASET_ONDISK2_CHECKER_H
#define ARKI_DATASET_ONDISK2_CHECKER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/ondisk2.h>
#include <arki/dataset/ondisk2/index.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;
class Matcher;
class Summary;

namespace dataset {
namespace ondisk2 {
struct CheckerSegment;

namespace writer {
class RealRepacker;
class RealFixer;
}

class Checker : public IndexedChecker
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;
    index::WIndex* idx;
    std::shared_ptr<dataset::CheckLock> lock;

public:
    Checker(std::shared_ptr<const ondisk2::Config> config);
    ~Checker();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments_tracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(CheckerConfig& opts) override;

    size_t vacuum(dataset::Reporter& reporter) override;
    void test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx) override;
    void test_remove_index(const std::string& relpath) override;

    friend class writer::RealRepacker;
    friend class writer::RealFixer;
    friend class CheckerSegment;
};

}
}
}
#endif

