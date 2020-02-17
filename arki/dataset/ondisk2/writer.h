#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/ondisk2.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;

namespace dataset {
namespace ondisk2 {
struct AppendSegment;

class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<ondisk2::Dataset> m_config;
    std::unique_ptr<AppendSegment> segment(const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> segment(const std::string& relpath);

public:
    Writer(std::shared_ptr<ondisk2::Dataset> config);
    virtual ~Writer();

    const ondisk2::Dataset& config() const override { return *m_config; }
    const ondisk2::Dataset& dataset() const override { return *m_config; }
    ondisk2::Dataset& dataset() override { return *m_config; }

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
