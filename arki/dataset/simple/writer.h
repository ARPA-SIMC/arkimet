#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/simple/manifest.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki::dataset::simple {

class AppendSegment;

class Writer : public DatasetAccess<simple::Dataset, segmented::Writer>
{
protected:
    manifest::Writer manifest;
    std::shared_ptr<simple::Dataset> m_config;

    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const Metadata& md, DataFormat format, std::shared_ptr<core::AppendLock> lock);
    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const std::filesystem::path& relpath, std::shared_ptr<core::AppendLock> lock);

    void invalidate_summary();

public:
    Writer(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Writer();

    std::string type() const override;

    void acquire_batch(metadata::InboundBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, metadata::InboundBatch& batch);
};

}
#endif
