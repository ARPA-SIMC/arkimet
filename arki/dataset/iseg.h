#ifndef ARKI_DATASET_ISEG_H
#define ARKI_DATASET_ISEG_H

#include <arki/dataset/segmented.h>
#include <arki/segment/index/iseg/config.h>
#include <arki/types/fwd.h>
#include <set>

namespace arki {
namespace dataset {
namespace iseg {

struct Dataset : public segmented::Dataset
{
    segment::index::iseg::Config iseg;
    std::string index_type;
    std::filesystem::path summary_cache_pathname;

    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;

    std::shared_ptr<segment::data::Reader> segment_reader(const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock) override;
};

}
}
}
#endif

