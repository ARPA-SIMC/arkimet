#ifndef ARKI_TYPES_BUNDLE_H
#define ARKI_TYPES_BUNDLE_H

#include <arki/core/fwd.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::types {

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
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read_header(core::NamedFileDescriptor& fd);

    /**
     * Read the bundle data after read_header has been called
     *
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read_data(core::NamedFileDescriptor& fd);

    /**
     * read_header and read_data together
     *
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read(core::NamedFileDescriptor& fd);

    /**
     * Read only the bundle header
     *
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read_header(core::AbstractInputFile& fd);

    /**
     * Read the bundle data after read_header has been called
     *
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read_data(core::AbstractInputFile& fd);

    /**
     * read_header and read_data together
     *
     * @return the number of bytes read, or 0 on end of file
     */
    size_t read(core::AbstractInputFile& fd);
};

} // namespace arki::types

#endif
