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

class Writer : public IndexedWriter
{
protected:
    std::shared_ptr<const simple::Config> m_config;
    index::Manifest* m_mft;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Writer(std::shared_ptr<const simple::Config> config);
    virtual ~Writer();

    const simple::Config& config() const override { return *m_config; }

    std::string type() const override;

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);
    void flush() override;

    virtual Pending test_writelock();

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

class Checker : public IndexedChecker
{
protected:
    std::shared_ptr<const simple::Config> m_config;
    index::Manifest* m_mft;

    /// Return a (shared) instance of the Segment for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(std::shared_ptr<const simple::Config> config);
    virtual ~Checker();

    const simple::Config& config() const override { return *m_config; }

    std::string type() const override;

    void indexSegment(const std::string& relpath, metadata::Collection&& contents) override;
    void rescanSegment(const std::string& relpath) override;
    size_t repackSegment(const std::string& relpath) override;
    void archiveSegment(const std::string& relpath) override;
    size_t removeSegment(const std::string& relpath, bool withData=false) override;
    size_t vacuum() override;
};

}
}
}
#endif
