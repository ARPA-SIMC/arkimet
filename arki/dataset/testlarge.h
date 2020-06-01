#ifndef ARKI_DATASET_TESTLARGE_H
#define ARKI_DATASET_TESTLARGE_H

/**
 * Virtual read only dataset that contains a lot of data. It uses no disk, and
 * the actual data payload is always zeroes.
 */

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace core {
class Time;
}

namespace dataset {
namespace testlarge {

struct Dataset : public dataset::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};


/**
 * Dataset that is always empty
 */
class Reader : public DatasetAccess<dataset::Dataset, dataset::Reader>
{
    bool generate(const core::Interval& interval, std::function<bool(std::unique_ptr<Metadata>)> out) const;

protected:
    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "empty"; }
};

}
}
}
#endif
