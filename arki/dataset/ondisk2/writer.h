#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

#include <arki/dataset/ondisk2.h>
#include <arki/dataset/impl.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;

namespace dataset {
namespace ondisk2 {
struct AppendSegment;

class Writer : public DatasetAccess<ondisk2::Dataset, segmented::Writer>
{
protected:
    std::unique_ptr<AppendSegment> segment(const segment::WriterConfig& writer_config, const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> segment(const segment::WriterConfig& writer_config, const std::string& relpath);

public:
    Writer(std::shared_ptr<ondisk2::Dataset> dataset);
    virtual ~Writer();

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;
    void remove(Metadata& md) override;

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};

}
}
}
#endif
