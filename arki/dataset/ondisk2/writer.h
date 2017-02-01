#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/ondisk2.h>
#include <arki/configfile.h>
#include <arki/dataset/index/contents.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;
class Matcher;
class Summary;

namespace dataset {
namespace ondisk2 {

namespace writer {
class RealRepacker;
class RealFixer;
}

class Writer : public IndexedWriter
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;
    index::WContents* idx;

    AcquireResult acquire_replace_never(Metadata& md);
    AcquireResult acquire_replace_always(Metadata& md);
    AcquireResult acquire_replace_higher_usn(Metadata& md);

public:
    Writer(std::shared_ptr<const ondisk2::Config> config);
    virtual ~Writer();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;
    virtual Pending test_writelock();

    /**
     * Iterate through the contents of the dataset, in depth-first order.
     */
    //void depthFirstVisit(Visitor& v);

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};


class ShardingWriter : public sharded::Writer<ondisk2::Config>
{
    using sharded::Writer<ondisk2::Config>::Writer;

    const ondisk2::Config& config() const override { return *m_config; }
    std::string type() const override;
};


class Checker : public IndexedChecker
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;
    index::WContents* idx;

public:
    Checker(std::shared_ptr<const ondisk2::Config> config);

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    void rescanSegment(const std::string& relpath) override;
    void indexSegment(const std::string& relpath, metadata::Collection&& contents) override;
    size_t repackSegment(const std::string& relpath, unsigned test_flags=0) override;
    size_t removeSegment(const std::string& relpath, bool withData=false) override;
    void releaseSegment(const std::string& relpath, const std::string& destpath) override;
    size_t vacuum() override;

    friend class writer::RealRepacker;
    friend class writer::RealFixer;
};

class ShardingChecker : public sharded::Checker<ondisk2::Config>
{
    using sharded::Checker<ondisk2::Config>::Checker;

    const ondisk2::Config& config() const override { return *m_config; }
    std::string type() const override;
};

}
}
}
#endif
