#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace ondisk2 {

struct Config : public dataset::IndexedConfig
{
    std::string summary_cache_pathname;
    std::string indexfile;
    std::string index_pathname;
    std::string index;
    std::string unique;

    Config(const Config&) = default;
    Config(const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(const core::cfg::Section& cfg);
};

}
}
}

#endif
