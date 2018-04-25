#ifndef ARKI_SEGMENT_MISSING_H
#define ARKI_SEGMENT_MISSING_H

#include <arki/segment.h>
#include <arki/segment/base.h>
#include <arki/core/fwd.h>
#include <string>
#include <vector>

namespace arki {
namespace segment {
namespace missing {

struct Segment : public arki::Segment
{
    using arki::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
};


struct Reader : public segment::BaseReader<Segment>
{
    using BaseReader<Segment>::BaseReader;

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};

}
}
}

#endif
