#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

#include <arki/dataset.h>

namespace arki {
namespace dataset {
namespace ondisk2 {

class Dataset : public dataset::Dataset
{
public:
    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};

}
}
}

#endif
