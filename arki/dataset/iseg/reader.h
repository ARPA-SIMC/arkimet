#ifndef ARKI_DATASET_ISEG_READER_H
#define ARKI_DATASET_ISEG_READER_H

/// Reader for iseg datasets with no duplicate checks

#include <arki/dataset/iseg.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {

class Reader : public segmented::Reader
{
protected:
    std::shared_ptr<const iseg::Config> m_config;

    /// List all existing segments matched by the reftime part of matcher
    void list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest);

public:
    Reader(std::shared_ptr<const iseg::Config> config);
    virtual ~Reader();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
