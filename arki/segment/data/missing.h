#ifndef ARKI_SEGMENT_MISSING_H
#define ARKI_SEGMENT_MISSING_H

#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/core/fwd.h>
#include <vector>

namespace arki::segment::data::missing {

struct Reader : public data::BaseReader<Data>
{
    using BaseReader<Data>::BaseReader;

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src, StreamOutput& out) override;
};

}

#endif
