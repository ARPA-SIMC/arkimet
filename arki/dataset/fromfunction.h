#ifndef ARKI_DATASET_FROMFUNCTION_H
#define ARKI_DATASET_FROMFUNCTION_H

#include <arki/dataset.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace fromfunction {

struct Dataset : public dataset::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
};


/**
 * Dataset that is always empty
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<dataset::Dataset> m_config;

public:
    std::function<bool(metadata_dest_func)> generator;

    Reader(std::shared_ptr<dataset::Dataset> config);
    virtual ~Reader();

    const dataset::Dataset& config() const override { return *m_config; }
    const dataset::Dataset& dataset() const override { return *m_config; }
    dataset::Dataset& dataset() override { return *m_config; }

    std::string type() const override { return "fromfunction"; }

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
};

}
}
}
#endif

