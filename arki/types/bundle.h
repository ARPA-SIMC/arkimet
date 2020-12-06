#ifndef ARKI_TYPES_BUNDLE_H
#define ARKI_TYPES_BUNDLE_H

#include <arki/core/fwd.h>
#include <string>
#include <vector>

namespace arki {
namespace types {

/**
 * Read a data bundle from a POSIX file descriptor
 */
struct Bundle
{
    /// Bundle signature
    std::string signature;
    /// Bundle version
    unsigned version;
    /// Data length
    size_t length;
    /// Bundle data
    std::vector<uint8_t> data;

    /**
     * Read only the bundle header
     *
     * @return true if a bundle header was read, false on end of file
     */
    bool read_header(core::NamedFileDescriptor& fd);

    /**
     * Read the bundle data after read_header has been called
     *
     * @return true if all bundle data was read, false on end of file
     */
    bool read_data(core::NamedFileDescriptor& fd);

    /**
     * read_header and read_data together
     *
     * @return true if all was read, false on end of file
     */
    bool read(core::NamedFileDescriptor& fd);

    /**
     * Read only the bundle header
     *
     * @return true if a bundle header was read, false on end of file
     */
    bool read_header(core::AbstractInputFile& fd);

    /**
     * Read the bundle data after read_header has been called
     *
     * @return true if all bundle data was read, false on end of file
     */
    bool read_data(core::AbstractInputFile& fd);

    /**
     * read_header and read_data together
     *
     * @return true if all was read, false on end of file
     */
    bool read(core::AbstractInputFile& fd);
};


}
}

#endif
