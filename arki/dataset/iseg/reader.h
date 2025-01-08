#ifndef ARKI_DATASET_ISEG_READER_H
#define ARKI_DATASET_ISEG_READER_H

/// Reader for iseg datasets with no duplicate checks

#include <arki/dataset/iseg.h>
#include <arki/dataset/impl.h>
#include <arki/dataset/index/summarycache.h>
#include <string>

namespace arki {
namespace dataset {
namespace iseg {

class Reader : public DatasetAccess<iseg::Dataset, segmented::Reader>
{
protected:
    index::SummaryCache scache;

    /// List all existing segments matched by the reftime part of matcher
    bool list_segments(const Matcher& matcher, std::function<bool(const std::filesystem::path& relpath)> dest);

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

    bool impl_query_data(const query::Data& q, metadata_dest_func dest) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    Reader(std::shared_ptr<iseg::Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    static bool is_dataset(const std::filesystem::path& dir);
};

}
}
}
#endif
