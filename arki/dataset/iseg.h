#ifndef ARKI_DATASET_ISEG_H
#define ARKI_DATASET_ISEG_H

#include <arki/dataset/segmented.h>
#include <arki/types/fwd.h>
#include <set>

namespace arki {
namespace dataset {
namespace iseg {

struct Config : public segmented::Config
{
    std::string format;
    bool smallfiles;
    std::string index_type;
    std::set<types::Code> index;
    std::set<types::Code> unique;
    std::string summary_cache_pathname;
    bool trace_sql;

    Config(const Config&) = default;
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

