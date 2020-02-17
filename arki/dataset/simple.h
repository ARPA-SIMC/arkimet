#ifndef ARKI_DATASET_SIMPLE_H
#define ARKI_DATASET_SIMPLE_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace simple {

struct Dataset : public dataset::indexed::Dataset
{
    std::string index_type;

    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;
};

}
}
}
#endif
