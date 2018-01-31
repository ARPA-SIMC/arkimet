#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <string>
#include <iosfwd>

namespace arki {
class ConfigFile;

namespace dataset {
namespace index {
class Manifest;
}

namespace simple {
class Reader;
class AppendSegment;

class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<const simple::Config> m_config;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    std::unique_ptr<AppendSegment> file(const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> file(const std::string& relname);

public:
    Writer(std::shared_ptr<const simple::Config> config);
    virtual ~Writer();

    const simple::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void acquire_batch(WriterBatch& batch, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);

    static void test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out);
};

}
}
}
#endif
