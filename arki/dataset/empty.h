#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/// Virtual read only dataset that is always empty

#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Dataset that is always empty
 */
class Empty : public Reader
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    Empty(std::shared_ptr<const Config> config);
    virtual ~Empty();

    const Config& config() const override { return *m_config; }
    std::string type() const override { return "empty"; }

    // Nothing to do: the dataset is always empty
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)>) override {}
    void query_summary(const Matcher& matcher, Summary& summary) override {}
    void query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out) override {}
};

}
}
#endif
