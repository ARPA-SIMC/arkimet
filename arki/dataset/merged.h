#ifndef ARKI_DATASET_MERGED_H
#define ARKI_DATASET_MERGED_H

/// dataset/merged - Access many datasets at the same time

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <string>
#include <vector>

namespace arki {
namespace dataset {
namespace merged {

struct Dataset : public dataset::Dataset
{
    std::vector<std::shared_ptr<dataset::Reader>> datasets;

    Dataset(std::shared_ptr<Session> session);

    /// Add a dataset to the group of datasets to merge
    void add_dataset(std::shared_ptr<dataset::Reader> ds);

    /// Add a dataset to the group of datasets to merge
    void add_dataset(std::shared_ptr<dataset::Dataset> ds);

    /// Add a dataset to the group of datasets to merge
    void add_dataset(const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
};

/**
 * Access multiple datasets together
 */
class Reader : public DatasetAccess<Dataset, dataset::Reader>
{
public:
    using DatasetAccess::DatasetAccess;
    virtual ~Reader();

    std::string type() const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
    void query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override;
};

}
}
}
#endif
