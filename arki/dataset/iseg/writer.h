#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/iseg.h>
#include <arki/dataset/index/summarycache.h>
#include <string>
#include <iosfwd>

namespace arki {
class ConfigFile;

namespace dataset {
namespace iseg {
class Reader;
class AIndex;
class AppendSegment;

class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<const iseg::Config> m_config;
    index::SummaryCache scache;

    /// Return an inserter for the given Metadata
    std::unique_ptr<AppendSegment> file(const Metadata& md);

    /// Return an inserter for the given relative pathname
    std::unique_ptr<AppendSegment> file(const std::string& relname);

public:
    Writer(std::shared_ptr<const iseg::Config> config);
    virtual ~Writer();

    const iseg::Config& config() const override { return *m_config; }

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
