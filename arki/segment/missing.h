#ifndef ARKI_SEGMENT_MISSING_H
#define ARKI_SEGMENT_MISSING_H

/// Base class for unix fd based read/write functions

#include <arki/segment.h>
#include <arki/core/fwd.h>
#include <string>
#include <vector>

namespace arki {
namespace segment {
namespace missing {

struct Reader : public segment::Reader
{
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() override;

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};

}
}
}

#endif
