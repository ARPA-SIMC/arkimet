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
    std::shared_ptr<const ondisk2::Config> m_config;
    std::unique_ptr<AppendSegment> segment(const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> segment(const std::string& relpath);

public:
    Writer(std::shared_ptr<const ondisk2::Config> config);
    virtual ~Writer();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;
    void remove(Metadata& md) override;

    /**
     * Iterate through the contents of the dataset, in depth-first order.
     */
    //void depthFirstVisit(Visitor& v);

    static void test_acquire(const core::cfg::Section& cfg, WriterBatch& batch, std::ostream& out);
};

}
}
}
#endif
