#ifndef ARKI_DATASET_OFFLINE_H
#define ARKI_DATASET_OFFLINE_H

#include <arki/dataset.h>
#include <arki/summary.h>
#include <string>

namespace arki {
class Matcher;

namespace dataset {
class DataQuery;
class ByteQuery;

struct OfflineConfig : public dataset::Config
{
    /**
     * Pathname to the .summary file which describes the data that is offline
     */
    std::string summary_pathname;

    OfflineConfig(const std::string& pathname);

    static std::shared_ptr<const OfflineConfig> create(const std::string& pathname);
};


/**
 * Archive that has been put offline (only a summary file is left)
 */
struct OfflineReader : public Reader
{
    std::shared_ptr<const OfflineConfig> m_config;
    Summary sum;

    OfflineReader(std::shared_ptr<const OfflineConfig> config);
    ~OfflineReader() {}

    const OfflineConfig& config() const override { return *m_config; }

    std::string type() const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;
};

}
}
#endif
