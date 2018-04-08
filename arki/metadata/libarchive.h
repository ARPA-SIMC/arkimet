#ifndef ARKI_METADATA_TAR_H
#define ARKI_METADATA_TAR_H

#include <arki/core/fwd.h>
#include <arki/metadata/collection.h>
#include <arki/libconfig.h>
#include <string>

struct archive;
struct archive_entry;

namespace arki {
namespace metadata {

/**
 * Output metadata and data using one of the archive formats supported by libarchive
 */
class LibarchiveOutput
{
protected:
#ifdef HAVE_LIBARCHIVE
    struct ::archive* a = nullptr;
    struct ::archive_entry* entry = nullptr;
    Collection mds;
    char filename_buf[255];
#endif

    void write_buffer(const std::vector<uint8_t>& buf);
    void append_metadata();

public:
    std::string format;
    core::NamedFileDescriptor& out;
    std::string subdir;
    bool with_metadata = true;

    LibarchiveOutput(const std::string& format, core::NamedFileDescriptor& out);
#ifdef HAVE_LIBARCHIVE
    ~LibarchiveOutput();
#endif

    /**
     * Append a metadata to the archive.
     *
     * @returns the index used to generate the file name
     */
    size_t append(const Metadata& md);
    void flush();
};

}
}

#endif
