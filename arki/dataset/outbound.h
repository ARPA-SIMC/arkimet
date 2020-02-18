#ifndef ARKI_DATASET_OUTBOUND_H
#define ARKI_DATASET_OUTBOUND_H

/// Local, non queryable, on disk dataset

#include <arki/dataset/segmented.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace outbound {

struct Dataset : public segmented::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
};

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Writer : public DatasetAccess<Dataset, segmented::Writer>
{
protected:
    void storeBlob(Metadata& md, const std::string& reldest, bool drop_cached_data_on_commit);

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Writer(std::shared_ptr<Dataset> config);
    virtual ~Writer();

    std::string type() const override;

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * @return true if the data is successfully stored in the dataset, else
     * false.  If false is returned, a note is added to the dataset explaining
     * the reason of the failure.
     */
    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;

    virtual void remove(Metadata& id);

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};

}
}
}
#endif
