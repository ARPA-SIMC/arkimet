#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/simple.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}
namespace simple {

class Reader : public DatasetAccess<simple::Dataset, segmented::Reader>
{
protected:
    Index* m_idx = nullptr;
    index::Manifest* m_mft = nullptr;

    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    Reader(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    static bool is_dataset(const std::string& dir);

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const { return m_idx != 0; }
};

}
}
}
#endif
