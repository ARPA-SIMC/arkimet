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

    std::shared_ptr<dataset::Reader> create_reader() override;
};

/**
 * Access multiple datasets together
 */
class Reader : public DatasetAccess<Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;
    void impl_fd_query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
    void impl_abstract_query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override;

public:
    using DatasetAccess::DatasetAccess;
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;
};

}
}
}
#endif
