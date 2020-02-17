#ifndef ARKI_DATASET_OFFLINE_H
#define ARKI_DATASET_OFFLINE_H

#include <arki/dataset.h>
#include <arki/summary.h>
#include <string>

namespace arki {
namespace dataset {
namespace offline {

struct Dataset : public dataset::Config
{
    /**
     * Pathname to the .summary file which describes the data that is offline
     */
    std::string summary_pathname;

    Dataset(std::shared_ptr<Session> session, const std::string& pathname);

    static std::shared_ptr<const Dataset> create(std::shared_ptr<Session> session, const std::string& pathname);
};


/**
 * Archive that has been put offline (only a summary file is left)
 */
struct Reader : public dataset::Reader
{
    std::shared_ptr<const Dataset> m_config;
    Summary sum;

    Reader(std::shared_ptr<const Dataset> config);
    ~Reader() {}

    const Dataset& config() const override { return *m_config; }

    std::string type() const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;
};

}
}
}
#endif
