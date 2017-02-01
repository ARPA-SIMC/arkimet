#ifndef ARKI_READER_H
#define ARKI_READER_H

/// Generic interface to read data files
#include <arki/libconfig.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstddef>
#include <sys/types.h>

namespace arki {

namespace types {
namespace source {
struct Blob;
}
}

namespace reader {

struct Reader
{
public:
    virtual ~Reader() {}

    virtual std::vector<uint8_t> read(const types::source::Blob& src) = 0;
    virtual bool is(const std::string& fname) const = 0;
};

class Registry
{
protected:
    std::unordered_map<std::string, std::weak_ptr<Reader>> cache;

    std::shared_ptr<Reader> instantiate(const std::string& abspath);

public:
    /**
     * Returns a reader for the file with the given absolute path
     */
    std::shared_ptr<Reader> reader(const std::string& abspath);

    /**
     * Remove from the cache the reader for the given file, if it exists.
     *
     * Calls to reader(abspath) after invalidation will instantiate a new
     * reader, even if some reader for this file is still used by some existing
     * metadata.
     *
     * This is useful to make sure that after a repack, new metadata will refer
     * to the new file.
     */
    void invalidate(const std::string& abspath);

    /**
     * Remove expired entries from the cache
     */
    void cleanup();

    /**
     * Get the global singleton registry
     */
    static Registry& get();
};

}
}
#endif
