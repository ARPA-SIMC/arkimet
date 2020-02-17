#ifndef ARKI_DATASET_ISEG_READER_H
#define ARKI_DATASET_ISEG_READER_H

/// Reader for iseg datasets with no duplicate checks

#include <arki/dataset/iseg.h>
#include <arki/dataset/index/summarycache.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {

class Reader : public segmented::Reader
{
protected:
    std::shared_ptr<const iseg::Dataset> m_config;
    index::SummaryCache scache;

    /// List all existing segments matched by the reftime part of matcher
    bool list_segments(const Matcher& matcher, std::function<bool(const std::string& relpath)> dest);

    /**
     * Compute the summary for the given month, and output it to \a
     * summary.
     *
     * Cache the result if possible, so that asking again will be much
     * quicker.
     */
    void summary_for_month(int year, int month, Summary& out);

    /**
     * Compute the summary for all the dataset.
     *
     * Cache the result if possible, so that asking again will be much
     * quicker.
     */
    void summary_for_all(Summary& out);

    void summary_from_indices(const Matcher& matcher, Summary& summary);

public:
    Reader(std::shared_ptr<const iseg::Dataset> config);
    virtual ~Reader();

    const iseg::Dataset& config() const override { return *m_config; }

    std::string type() const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
