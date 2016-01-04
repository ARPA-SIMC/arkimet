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

/**
 * Archive that has been put offline (only a summary file is left)
 */
struct OfflineReader : public Reader
{
    std::string fname;
    Summary sum;

    OfflineReader(const std::string& fname);
    ~OfflineReader() {}

    std::string type() const override;

    void query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) override;
};

}
}
#endif
