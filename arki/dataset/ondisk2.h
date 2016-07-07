#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace ondisk2 {

struct Config : public dataset::IndexedConfig
{
    bool smallfiles;
    std::string summary_cache_pathname;
    std::string index_pathname;
    std::string index;
    std::string unique;

    Config(const ConfigFile& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

}
}
}

#endif
