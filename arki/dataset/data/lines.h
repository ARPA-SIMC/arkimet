#ifndef ARKI_DATA_LINES_H
#define ARKI_DATA_LINES_H

/// Read/write functions for data blobs with newline separators
#include <arki/libconfig.h>
#include <arki/dataset/data/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace data {
namespace lines {

class Segment : public fd::Segment
{
public:
    Segment(const std::string& relname, const std::string& absname);

    void write(const std::vector<uint8_t>& buf) override;

    off_t append(Metadata& md) override;
    off_t append(const std::vector<uint8_t>& buf) override;
    Pending append(Metadata& md, off_t* ofs) override;

    FileState check(const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;
};

class OstreamWriter : public data::OstreamWriter
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
