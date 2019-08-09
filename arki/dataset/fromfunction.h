#ifndef ARKI_DATASET_FROMFUNCTION_H
#define ARKI_DATASET_FROMFUNCTION_H

#include <arki/dataset.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace fromfunction {

struct Config : public dataset::Config
{
    Config(const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;

    static std::shared_ptr<const Config> create(const core::cfg::Section& cfg);
};


/**
 * Dataset that is always empty
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

public:
    std::function<bool(metadata_dest_func)> generator;

    Reader(std::shared_ptr<const dataset::Config> config);
    virtual ~Reader();

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "fromfunction"; }

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
};

}
}
}
#endif

