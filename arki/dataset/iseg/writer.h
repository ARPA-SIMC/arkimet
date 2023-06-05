#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/iseg.h>
#include <arki/dataset/impl.h>
#include <arki/dataset/index/summarycache.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {
class AppendSegment;

class Writer : public DatasetAccess<iseg::Dataset, segmented::Writer>
{
protected:
    index::SummaryCache scache;

    /// Return the relative path of the segment for this metadata
    std::string get_relpath(const Metadata& md);

    /// Return an inserter for the given Metadata
    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const Metadata& md);

    /// Return an inserter for the given relative pathname
    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const std::string& relpath);

public:
    Writer(std::shared_ptr<iseg::Dataset> config);
    virtual ~Writer();

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};


}
}
}
#endif
