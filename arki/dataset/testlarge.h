#ifndef ARKI_DATASET_TESTLARGE_H
#define ARKI_DATASET_TESTLARGE_H

/**
 * Virtual read only dataset that contains a lot of data. It uses no disk, and
 * the actual data payload is always zeroes.
 */

#include <arki/dataset.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace core {
class Time;
}

namespace dataset {
namespace testlarge {

struct Dataset : public dataset::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};


/**
 * Dataset that is always empty
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<dataset::Dataset> m_config;

    bool generate(const core::Time& begin, const core::Time& until, std::function<bool(std::unique_ptr<Metadata>)> out) const;

public:
    Reader(std::shared_ptr<dataset::Dataset> config);
    virtual ~Reader();

    const dataset::Dataset& config() const override { return *m_config; }
    const dataset::Dataset& dataset() const override { return *m_config; }
    dataset::Dataset& dataset() override { return *m_config; }

    std::string type() const override { return "empty"; }

    // Nothing to do: the dataset is always empty
    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}
}
}
#endif
