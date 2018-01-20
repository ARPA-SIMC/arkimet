#ifndef ARKI_READER_H
#define ARKI_READER_H

#include <arki/core/fwd.h>
#include <string>
#include <memory>
#include <vector>
#include <cstddef>
#include <sys/types.h>

namespace arki {

namespace types {
namespace source {
struct Blob;
}
}

/// Generic interface to read data files
struct Reader
{
public:
    virtual ~Reader() {}

    virtual std::vector<uint8_t> read(const types::source::Blob& src) = 0;
    virtual size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) = 0;

    static std::shared_ptr<Reader> create_new(const std::string& abspath, std::shared_ptr<core::Lock> lock);
};

}
#endif
