#ifndef ARKI_DATASET_SIMPLE_H
#define ARKI_DATASET_SIMPLE_H

#include <arki/dataset/segmented.h>

namespace arki {
namespace dataset {
class Index;

namespace simple {

struct Dataset : public dataset::segmented::Dataset
{
    std::string index_type;

    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;

    std::shared_ptr<dataset::ReadLock> read_lock_dataset() const;
    std::shared_ptr<dataset::AppendLock> append_lock_dataset() const;
    std::shared_ptr<dataset::CheckLock> check_lock_dataset() const;
};

}
}
}
#endif
