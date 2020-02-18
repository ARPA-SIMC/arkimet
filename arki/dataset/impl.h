#ifndef ARKI_DATASET_IMPL_H
#define ARKI_DATASET_IMPL_H

#include <memory>

namespace arki {
namespace dataset {

template<typename Dataset, typename Base>
struct DatasetAccess: public Base
{
protected:
    std::shared_ptr<Dataset> m_dataset;

public:
    typedef Dataset dataset_type;

    DatasetAccess(std::shared_ptr<Dataset> dataset) : m_dataset(dataset) {}

    const Dataset& dataset() const override { return *m_dataset; }
    Dataset& dataset() override { return *m_dataset; }
};

}
}

#endif
