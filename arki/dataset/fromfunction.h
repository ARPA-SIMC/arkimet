#ifndef ARKI_DATASET_FROMFUNCTION_H
#define ARKI_DATASET_FROMFUNCTION_H

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {
namespace fromfunction {

struct Dataset : public dataset::Dataset
{
    using dataset::Dataset::Dataset;

    std::shared_ptr<dataset::Reader> create_reader() override;
};


/**
 * Dataset that is always empty
 */
class Reader : public DatasetAccess<dataset::Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const query::Data& q, metadata_dest_func dest) override;

public:
    using DatasetAccess::DatasetAccess;

    std::function<bool(metadata_dest_func)> generator;

    std::string type() const override { return "fromfunction"; }

    core::Interval get_stored_time_interval() override;
};

}
}
}
#endif

