#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}
namespace simple {

class Reader : public DatasetAccess<simple::Dataset, indexed::Reader>
{
protected:
    index::Manifest* m_mft = nullptr;

public:
    Reader(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
