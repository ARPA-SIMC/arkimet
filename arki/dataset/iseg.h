#ifndef ARKI_DATASET_ISEG_H
#define ARKI_DATASET_ISEG_H

#include <arki/dataset/segmented.h>
#include <arki/types/fwd.h>
#include <set>

namespace arki {
namespace dataset {
namespace iseg {

struct Dataset : public segmented::Dataset
{
    std::string format;
    std::string index_type;
    std::set<types::Code> index;
    std::set<types::Code> unique;
    std::string summary_cache_pathname;
    bool trace_sql;

    Dataset(const Dataset&) = default;
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;
};

}
}
}
#endif

