#ifndef ARKI_DATASET_SEGMENT_CONCAT_H
#define ARKI_DATASET_SEGMENT_CONCAT_H

#include <arki/libconfig.h>
#include <arki/dataset/segment/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace concat {

class Segment : public fd::Segment
{
protected:
    void test_add_padding(unsigned size) override;

public:
    Segment(const std::string& relname, const std::string& absname);

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

class HoleSegment : public concat::Segment
{
public:
    HoleSegment(const std::string& relname, const std::string& absname)
        : Segment(relname, absname) {}

    void write(off_t wrpos, const std::vector<uint8_t>& buf) override;

    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

}
}
}
}
#endif

