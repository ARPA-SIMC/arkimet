#ifndef ARKI_SEGMENT_MISSING_H
#define ARKI_SEGMENT_MISSING_H

#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/core/fwd.h>
#include <vector>

namespace arki::segment::data::missing {

struct Segment : public arki::segment::Segment
{
    using arki::segment::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;
    std::shared_ptr<data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<data::Checker> checker() const override;
};


struct Reader : public data::BaseReader<Segment>
{
    using BaseReader<Segment>::BaseReader;

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src, StreamOutput& out) override;
};

}

#endif
