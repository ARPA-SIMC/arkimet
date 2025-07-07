#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/impl.h>
#include <arki/dataset/simple.h>
#include <arki/dataset/simple/manifest.h>
#include <string>

namespace arki::dataset::simple {

class Reader : public DatasetAccess<simple::Dataset, segmented::Reader>
{
protected:
    manifest::Reader manifest;

    void query_segments_for_summary(const Matcher& matcher, Summary& summary,
                                    std::shared_ptr<core::ReadLock> lock);
    bool impl_query_data(const query::Data& q,
                         metadata_dest_func dest) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    explicit Reader(std::shared_ptr<simple::Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    static bool is_dataset(const std::filesystem::path& dir);

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const;
};

} // namespace arki::dataset::simple
#endif
