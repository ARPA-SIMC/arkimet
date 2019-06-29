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

struct Config : public dataset::Config
{
    Config(const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(const core::cfg::Section& cfg);
};


/**
 * Dataset that is always empty
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

    bool generate(const core::Time& begin, const core::Time& until, std::function<bool(std::unique_ptr<Metadata>)> out) const;

public:
    Reader(std::shared_ptr<const dataset::Config> config);
    virtual ~Reader();

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "empty"; }

    // Nothing to do: the dataset is always empty
    bool query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)>) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}
}
}
#endif
