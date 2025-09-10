#ifndef ARKI_DATASET_OUTBOUND_H
#define ARKI_DATASET_OUTBOUND_H

/// Local, non queryable, on disk dataset

#include <arki/dataset/impl.h>
#include <arki/dataset/segmented.h>
#include <arki/metadata/fwd.h>
#include <string>

namespace arki::dataset::outbound {

struct SegmentSession : public segment::Session
{
public:
    using segment::Session::Session;

    std::shared_ptr<segment::Writer>
    segment_writer(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<core::AppendLock> lock) const override;
};

struct Dataset : public segmented::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;

    std::shared_ptr<core::AppendLock> append_lock_dataset() const;
};

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Writer : public DatasetAccess<Dataset, segmented::Writer>
{
public:
    // Initialise the dataset with the information from the configurationa in
    // 'cfg'
    explicit Writer(std::shared_ptr<Dataset> config);
    virtual ~Writer();

    std::string type() const override;

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * @return true if the data is successfully stored in the dataset, else
     * false.  If false is returned, a note is added to the dataset explaining
     * the reason of the failure.
     */
    void acquire_batch(metadata::InboundBatch& batch,
                       const AcquireConfig& cfg = AcquireConfig()) override;

    static void test_acquire(std::shared_ptr<Session> session,
                             const core::cfg::Section& cfg,
                             metadata::InboundBatch& batch);
};

} // namespace arki::dataset::outbound
#endif
