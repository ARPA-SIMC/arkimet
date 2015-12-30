#ifndef ARKI_DATASET_SIMLE_DATAFILE_H
#define ARKI_DATASET_SIMLE_DATAFILE_H

#include <string>
#include <arki/summary.h>
#include <arki/dataset/segment.h>
#include <arki/metadata/collection.h>

namespace arki {
class Metadata;

namespace dataset {
namespace simple {

namespace datafile {

/// Accumulate metadata and summaries while writing
struct MdBuf : public Segment::Payload
{
    std::string pathname;
    std::string dirname;
    std::string basename;
    bool flushed;
    metadata::Collection mds;
    Summary sum;

    MdBuf(const std::string& pathname);
    ~MdBuf();

    void add(const Metadata& md);
    void flush();
};

}

}
}
}
#endif
