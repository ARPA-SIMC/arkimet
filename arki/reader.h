#ifndef ARKI_READER_H
#define ARKI_READER_H

#include <arki/file.h>
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
    virtual size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) = 0;

    static std::shared_ptr<Reader> for_missing(const std::string& abspath);
    static std::shared_ptr<Reader> for_file(const std::string& abspath);
    static std::shared_ptr<Reader> for_dir(const std::string& abspath);
    static std::shared_ptr<Reader> for_auto(const std::string& abspath);

    /**
     * Empty the reader caches, used after a segment gets repacked, to prevent
     * reading an old segment using new offsets
     */
    static void reset();

    /**
     * Count the number of cached readers
     */
    static unsigned test_count_cached();
};

}
#endif
