#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/iseg.h>
#include <arki/dataset/index/summarycache.h>
#include <string>

namespace arki {
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
    std::unique_ptr<AppendSegment> file(const std::string& relpath);

public:
    Writer(std::shared_ptr<const iseg::Config> config);
    virtual ~Writer();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;
    void remove(Metadata& md);

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};


}
}
}
#endif
