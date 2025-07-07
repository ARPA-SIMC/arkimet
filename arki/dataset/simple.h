#ifndef ARKI_DATASET_SIMPLE_H
#define ARKI_DATASET_SIMPLE_H

#include <arki/dataset/segmented.h>

namespace arki {
namespace dataset {
class Index;

namespace simple {

struct SegmentSession : public segment::Session
{
public:
    using segment::Session::Session;

    std::shared_ptr<segment::Reader>
    segment_reader(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Writer>
    segment_writer(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<core::AppendLock> lock) const override;
    std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const override;
};

struct Dataset : public dataset::segmented::Dataset
{
    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;

    std::shared_ptr<core::ReadLock> read_lock_dataset() const;
    std::shared_ptr<core::AppendLock> append_lock_dataset() const;
    std::shared_ptr<core::CheckLock> check_lock_dataset() const;
};

} // namespace simple
} // namespace dataset
} // namespace arki
#endif
