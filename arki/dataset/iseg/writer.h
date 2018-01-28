#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/iseg.h>
#include <arki/dataset/index/summarycache.h>
#include <arki/configfile.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;

namespace dataset {
namespace iseg {
class Reader;
class AIndex;
class WIndex;
class AppendSegment;
class CheckerSegment;

class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<const iseg::Config> m_config;
    index::SummaryCache scache;

    /// Return an inserter for the given Metadata
    std::unique_ptr<AppendSegment> file(const Metadata& md);

    /// Return an inserter for the given relative pathname
    std::unique_ptr<AppendSegment> file(const std::string& relname);

    //void acquire_batch_replace_never(std::vector<std::shared_ptr<WriterBatchElement>>& batch);

public:
    Writer(std::shared_ptr<const iseg::Config> config);
    virtual ~Writer();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);

    static void test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out);
};


class Checker : public segmented::Checker
{
protected:
    std::shared_ptr<const iseg::Config> m_config;

    void list_segments(std::function<void(const std::string& relpath)> dest);
    void list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest);

public:
    Checker(std::shared_ptr<const iseg::Config> config);

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void check_issue51(dataset::Reporter& reporter, bool fix=false) override;
    size_t vacuum(dataset::Reporter& reporter) override;

    void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx=1) override;
    void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx=0) override;
    void test_corrupt_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_truncate_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx) override;
    void test_rename(const std::string& relpath, const std::string& new_relpath) override;
    void test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx) override;
    void test_remove_index(const std::string& relpath) override;

    friend class CheckerSegment;
};

}
}
}
#endif
