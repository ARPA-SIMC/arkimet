#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <string>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}
namespace simple {

class Reader : public indexed::Reader
{
protected:
    std::shared_ptr<const simple::Dataset> m_config;
    index::Manifest* m_mft = nullptr;

public:
    Reader(std::shared_ptr<const simple::Dataset> config);
    virtual ~Reader();

    const simple::Dataset& config() const override { return *m_config; }

    std::string type() const override;

    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
