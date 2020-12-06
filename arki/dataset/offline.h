#ifndef ARKI_DATASET_OFFLINE_H
#define ARKI_DATASET_OFFLINE_H

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <arki/summary.h>
#include <string>

namespace arki {
namespace dataset {
namespace offline {

struct Dataset : public dataset::Dataset
{
    /**
     * Pathname to the .summary file which describes the data that is offline
     */
    std::string summary_pathname;

    Dataset(std::weak_ptr<Session> session, const std::string& pathname);

    std::shared_ptr<dataset::Reader> create_reader() override;
};


/**
 * Archive that has been put offline (only a summary file is left)
 */
struct Reader : public DatasetAccess<Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    Summary sum;

    Reader(std::shared_ptr<Dataset> dataset);
    ~Reader() {}

    std::string type() const override;

    core::Interval get_stored_time_interval() override;
};

}
}
}
#endif
