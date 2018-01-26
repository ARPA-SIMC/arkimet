#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/configfile.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;

namespace dataset {
namespace index {
class Manifest;
}

namespace simple {
class Reader;
class CheckerSegment;

namespace datafile {
class MdBuf;
}

class Writer : public IndexedWriter
{
protected:
    std::shared_ptr<const simple::Config> m_config;
    index::Manifest* m_mft;
    std::shared_ptr<dataset::Lock> lock;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    std::unique_ptr<datafile::MdBuf> file(const Metadata& md, const std::string& format);

public:
    Writer(std::shared_ptr<const simple::Config> config);
    virtual ~Writer();

    const simple::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);
    void flush() override;

    virtual Pending test_writelock();

    static void test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out);
};

class Checker : public IndexedChecker
{
protected:
    std::shared_ptr<const simple::Config> m_config;
    index::Manifest* m_mft;
    std::shared_ptr<dataset::CheckLock> lock;

    /// Return a (shared) instance of the Segment for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(std::shared_ptr<const simple::Config> config);
    virtual ~Checker();

    const simple::Config& config() const override { return *m_config; }

    std::string type() const override;

    std::unique_ptr<segmented::CheckerSegment> segment(const std::string& relpath) override;
    std::unique_ptr<segmented::CheckerSegment> segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock) override;
    void segments(std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void segments_untracked(std::function<void(segmented::CheckerSegment& relpath)>) override;
    void segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)>) override;
    void remove_all(dataset::Reporter& reporter, bool writable=false) override;
    void remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
    void check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick) override;

    size_t vacuum(dataset::Reporter& reporter) override;
    void test_remove_index(const std::string& relpath) override;
    void test_rename(const std::string& relpath, const std::string& new_relpath) override;
    void test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx) override;

    friend class CheckerSegment;
};

}
}
}
#endif
