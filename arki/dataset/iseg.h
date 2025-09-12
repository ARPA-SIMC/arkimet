#ifndef ARKI_DATASET_ISEG_H
#define ARKI_DATASET_ISEG_H

#include <arki/dataset/segmented.h>
#include <arki/segment/iseg/fwd.h>
#include <arki/types/fwd.h>
#include <set>

namespace arki::dataset::iseg {

struct Dataset : public segmented::Dataset
{
    std::shared_ptr<arki::segment::iseg::Session> iseg_segment_session;
    std::filesystem::path summary_cache_pathname;

    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;

    /**
     * Create/open a dataset-wide lockfile, returning the Lock instance
     */
    std::shared_ptr<core::ReadLock>
    read_lock_segment(const std::filesystem::path& relpath) const;
    std::shared_ptr<core::AppendLock>
    append_lock_segment(const std::filesystem::path& relpath) const;
    std::shared_ptr<core::CheckLock>
    check_lock_segment(const std::filesystem::path& relpath) const;
};

} // namespace arki::dataset::iseg
#endif
