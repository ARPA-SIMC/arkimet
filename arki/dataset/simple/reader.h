#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/local.h>

#include <string>
#include <iosfwd>

namespace arki {
class ConfigFile;
class Matcher;

namespace dataset {
namespace maintenance {
class MaintFileVisitor;
}

namespace index {
struct Manifest;
}

namespace simple {

class Reader : public SegmentedLocal
{
protected:
    index::Manifest* m_mft;

public:
    Reader(const ConfigFile& cfg);
    virtual ~Reader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    size_t produce_nth(metadata::Eater& cons, size_t idx=0) override;

    void maintenance(maintenance::MaintFileVisitor& v);

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
