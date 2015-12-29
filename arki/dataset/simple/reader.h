#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/indexed.h>
#include <arki/types/time.h>
#include <string>

namespace arki {
namespace dataset {
namespace index {
class Manifest;
}
namespace simple {

class Reader : public IndexedReader
{
protected:
    index::Manifest* m_mft = nullptr;

public:
    Reader(const ConfigFile& cfg);
    virtual ~Reader();

    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) override;

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
