#ifndef ARKI_DATASET_ONDISK2_READER_H
#define ARKI_DATASET_ONDISK2_READER_H

#include <arki/dataset/ondisk2.h>
#include <string>
#include <map>
#include <vector>

namespace arki {
namespace dataset {
namespace ondisk2 {

class Reader : public IndexedReader
{
protected:
    std::shared_ptr<const ondisk2::Config> m_config;

public:
    Reader(std::shared_ptr<const ondisk2::Config> config);
    virtual ~Reader();

    const ondisk2::Config& config() const override { return *m_config; }

    std::string type() const override;
};

class ShardingReader : public sharded::Reader<ondisk2::Config>
{
    using sharded::Reader<ondisk2::Config>::Reader;

    const ondisk2::Config& config() const override { return *m_config; }
    std::string type() const override;
};

}
}
}
#endif
