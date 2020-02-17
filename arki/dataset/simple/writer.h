#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <string>

namespace arki {
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
    std::shared_ptr<simple::Dataset> m_config;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    std::unique_ptr<AppendSegment> file(const Metadata& md, const std::string& format);
    std::unique_ptr<AppendSegment> file(const std::string& relpath);

public:
    Writer(std::shared_ptr<simple::Dataset> config);
    virtual ~Writer();

    const simple::Dataset& config() const override { return *m_config; }
    const simple::Dataset& dataset() const override { return *m_config; }
    simple::Dataset& dataset() override { return *m_config; }

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
