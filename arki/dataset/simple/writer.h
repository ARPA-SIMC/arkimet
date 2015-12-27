#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/indexed.h>
#include <arki/configfile.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;

namespace dataset {
class TargetFile;

namespace index {
class Manifest;
}

namespace simple {
class Reader;
class Datafile;

class Writer : public IndexedWriter
{
protected:
    index::Manifest* m_mft;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    segment::Segment* file(const Metadata& md, const std::string& format);

public:
    Writer(const ConfigFile& cfg);
    virtual ~Writer();

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);
    void flush() override;

    virtual Pending test_writelock();

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

class Checker : public IndexedChecker
{
protected:
    index::Manifest* m_mft;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    segment::Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(const ConfigFile& cfg);
    virtual ~Checker();

    void maintenance(segment::state_func v, bool quick=true) override;
    void removeAll(std::ostream& log, bool writable) override;
    void indexFile(const std::string& relpath, metadata::Collection&& contents) override;
    void rescanFile(const std::string& relpath) override;
    size_t repackFile(const std::string& relpath) override;
    void archiveFile(const std::string& relpath) override;
    size_t removeFile(const std::string& relpath, bool withData=false) override;
    size_t vacuum() override;
};

}
}
}
#endif
