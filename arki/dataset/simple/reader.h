#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/// Reader for simple datasets with no duplicate checks

#include <arki/dataset/indexed.h>

#include <string>
#include <iosfwd>

namespace arki {
namespace dataset {
namespace simple {

class Reader : public IndexedReader
{
public:
    Reader(const ConfigFile& cfg);
    virtual ~Reader();

    static bool is_dataset(const std::string& dir);
};

}
}
}
#endif
