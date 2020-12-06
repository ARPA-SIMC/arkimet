#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace ondisk2 {

class Dataset : public dataset::indexed::Dataset
{
public:
    std::string summary_cache_pathname;
    std::string indexfile;
    std::string index_pathname;
    std::string index;
    std::string unique;

    Dataset(const Dataset&) = default;
    Dataset(std::weak_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};

}
}
}

#endif
