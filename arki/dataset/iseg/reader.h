#ifndef ARKI_DATASET_ISEG_READER_H
#define ARKI_DATASET_ISEG_READER_H

/// Reader for iseg datasets with no duplicate checks

#include <arki/dataset/iseg.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {

class Reader : public segmented::Reader
{
protected:
    std::shared_ptr<const iseg::Config> m_config;

public:
    Reader(std::shared_ptr<const iseg::Config> config);
    virtual ~Reader();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
