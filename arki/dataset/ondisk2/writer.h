#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/ondisk2.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/configfile.h>
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

class Writer : public IndexedWriter
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;
    index::WContents* idx;
    std::shared_ptr<dataset::Lock> lock;

    WriterAcquireResult acquire_replace_never(Metadata& md);
    WriterAcquireResult acquire_replace_always(Metadata& md);
    WriterAcquireResult acquire_replace_higher_usn(Metadata& md);

public:
    Writer(std::shared_ptr<const ondisk2::Config> config);
    virtual ~Writer();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;
    virtual Pending test_writelock();

    /**
     * Iterate through the contents of the dataset, in depth-first order.
     */
    //void depthFirstVisit(Visitor& v);

    static void test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out);
};


class Checker : public IndexedChecker
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;
    index::WContents* idx;
    std::shared_ptr<dataset::CheckLock> lock;

public:
    Checker(std::shared_ptr<const ondisk2::Config> config);
    ~Checker();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void remove_all(dataset::Reporter& reporter, bool writable=false) override;
    void remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
    void check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick) override;

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
