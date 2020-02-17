#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace ondisk2 {

struct Dataset : public dataset::indexed::Dataset
{
    std::string summary_cache_pathname;
    std::string indexfile;
    std::string index_pathname;
    std::string index;
    std::string unique;

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
