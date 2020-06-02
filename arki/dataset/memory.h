#ifndef ARKI_DATASET_MEMORY_H
#define ARKI_DATASET_MEMORY_H

/// dataset/memory - Dataset interface for metadata::Collection

#include <arki/metadata/collection.h>
#include <arki/dataset.h>
#include <arki/dataset/impl.h>

namespace arki {
struct Summary;

namespace dataset {
namespace memory {

struct Dataset : public dataset::Dataset, public metadata::Collection
{
    Dataset(std::shared_ptr<Session> session);

    std::shared_ptr<dataset::Reader> create_reader() override;
};

/**
 * Consumer that collects all metadata into a vector
 */
struct Reader : public DatasetAccess<Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

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

