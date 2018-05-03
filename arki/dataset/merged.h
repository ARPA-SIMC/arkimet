#ifndef ARKI_DATASET_MERGED_H
#define ARKI_DATASET_MERGED_H

/// dataset/merged - Access many datasets at the same time

#include <arki/dataset.h>
#include <string>
#include <vector>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Access multiple datasets together
 */
class Merged : public Reader
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    std::vector<std::shared_ptr<Reader>> datasets;

    Merged();
    virtual ~Merged();

    const Config& config() const override { return *m_config; }
    std::string type() const override;

    /// Add a dataset to the group of datasets to merge
    void add_dataset(std::shared_ptr<Reader> ds);

    /// Add a dataset to the group of datasets to merge
    void add_dataset(const ConfigFile& cfg);

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
};

}
}
#endif
