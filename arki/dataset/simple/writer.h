#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {

namespace simple {
class AppendSegment;

class Writer : public DatasetAccess<simple::Dataset, segmented::Writer>
{
protected:
    std::shared_ptr<simple::Dataset> m_config;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> file(const segment::WriterConfig& writer_config, const std::string& relpath);

public:
    Writer(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Writer();

    std::string type() const override;

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;
    void remove(const metadata::Collection&) override;

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};

}
}
}
#endif
