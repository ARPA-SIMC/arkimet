#ifndef ARKI_DATASET_ONDISK2_READER_H
#define ARKI_DATASET_ONDISK2_READER_H

#include <arki/dataset/indexed.h>
#include <string>
#include <map>
#include <vector>

namespace arki {
namespace dataset {
namespace ondisk2 {

class Reader : public IndexedReader
{
public:
    Reader(const ConfigFile& cfg);
    virtual ~Reader();
};

}
}
}
#endif
