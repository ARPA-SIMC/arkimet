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
#ifdef HAVE_LIBARCHIVE
    struct ::archive* a = nullptr;
    struct ::archive_entry* entry = nullptr;
    Collection mds;
    char filename_buf[255];
#endif

public:
    std::string format;
    core::NamedFileDescriptor& out;

    LibarchiveOutput(const std::string& format, core::NamedFileDescriptor& out);
#ifdef HAVE_LIBARCHIVE
    ~LibarchiveOutput();
#endif

    void append(const Metadata& md);
    void flush();
};

}
}

#endif
