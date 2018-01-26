#ifndef ARKI_DATASET_SIMLE_DATAFILE_H
#define ARKI_DATASET_SIMLE_DATAFILE_H

#include <string>
#include <arki/summary.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/segment.h>
#include <arki/metadata/collection.h>
#include <arki/utils/sys.h>

namespace arki {
class Metadata;

namespace dataset {
namespace simple {

namespace datafile {

/// Accumulate metadata and summaries while writing
struct MdBuf
{
    std::shared_ptr<segment::Writer> segment;
    utils::sys::Path dir;
    std::string basename;
    std::string pathname;
    bool flushed;
    metadata::Collection mds;
    Summary sum;

    MdBuf(const std::string& pathname, std::shared_ptr<core::Lock> lock);
    MdBuf(std::shared_ptr<segment::Writer> segment, std::shared_ptr<core::Lock> lock);
    ~MdBuf();

    void add(const Metadata& md, const types::source::Blob& source);
    void flush();
};

}

}
}
}
#endif
