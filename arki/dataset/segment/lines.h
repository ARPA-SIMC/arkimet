#ifndef ARKI_DATASET_SEGMENT_LINES_H
#define ARKI_DATASET_SEGMENT_LINES_H

/// Read/write functions for data blobs with newline separators
#include <arki/libconfig.h>
#include <arki/dataset/segment/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace lines {

class Segment : public fd::Segment
{
protected:
    void test_add_padding(unsigned size) override;

public:
    Segment(const std::string& relname, const std::string& absname);

    void write(off_t wrpos, const std::vector<uint8_t>& buf) override;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

}
}
}
}
#endif
