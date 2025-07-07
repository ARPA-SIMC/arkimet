#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/impl.h>
#include <arki/dataset/index/summarycache.h>
#include <arki/dataset/iseg.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {
class AppendSegment;

class Writer : public DatasetAccess<iseg::Dataset, segmented::Writer>
{
protected:
    index::SummaryCache scache;

public:
    explicit Writer(std::shared_ptr<iseg::Dataset> config);
    virtual ~Writer();

    std::string type() const override;

    void acquire_batch(metadata::InboundBatch& batch,
                       const AcquireConfig& cfg = AcquireConfig()) override;

    static void test_acquire(std::shared_ptr<Session> session,
                             const core::cfg::Section& cfg,
                             metadata::InboundBatch& batch);
};

} // namespace iseg
} // namespace dataset
} // namespace arki
#endif
