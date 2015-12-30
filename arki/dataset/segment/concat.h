#ifndef ARKI_DATASET_DATA_CONCAT_H
#define ARKI_DATASET_DATA_CONCAT_H

#include <arki/libconfig.h>
#include <arki/dataset/segment/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace concat {

class Segment : public fd::Segment
{
public:
    Segment(const std::string& relname, const std::string& absname);

    off_t append(Metadata& md) override;
    off_t append(const std::vector<uint8_t>& buf) override;
    Pending append(Metadata& md, off_t* ofs) override;

    State check(const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;
};

class HoleSegment : public concat::Segment
{
public:
    HoleSegment(const std::string& relname, const std::string& absname)
        : Segment(relname, absname) {}

    void write(const std::vector<uint8_t>& buf) override;

    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;
};

class OstreamWriter : public segment::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    size_t stream(Metadata& md, std::ostream& out) const override;
    size_t stream(Metadata& md, int out) const override;
};

}
}
}
}

#endif

