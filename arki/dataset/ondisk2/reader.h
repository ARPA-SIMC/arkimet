#ifndef ARKI_DATASET_ONDISK2_READER_H
#define ARKI_DATASET_ONDISK2_READER_H

#include <arki/dataset/ondisk2.h>
#include <arki/dataset/impl.h>
#include <string>
#include <map>
#include <vector>

namespace arki {
namespace dataset {
namespace ondisk2 {

class Reader : public DatasetAccess<ondisk2::Dataset, indexed::Reader>
{
public:
    Reader(std::shared_ptr<ondisk2::Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;
};

}
}
}
#endif
