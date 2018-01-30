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
    index::Manifest* m_mft;
    std::shared_ptr<dataset::Lock> lock;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    std::unique_ptr<AppendSegment> file(const Metadata& md, const std::string& format);

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

}
}
}
#endif
