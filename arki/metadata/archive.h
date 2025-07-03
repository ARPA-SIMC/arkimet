#ifndef ARKI_METADATA_LIBARCHIVE_H
#define ARKI_METADATA_LIBARCHIVE_H

#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/metadata/fwd.h>
#include <string>
#include <filesystem>
#include <memory>

namespace arki {
namespace metadata {

/**
 * Output metadata and data into archive files
 */
class ArchiveOutput
{
public:
    virtual ~ArchiveOutput();

    /// Set the subdirectory prefix for archived files
    virtual void set_subdir(const std::filesystem::path& subdir) = 0;

    /**
     * Append a metadata to the archive.
     *
     * @returns the index used to generate the file name
     */
    virtual size_t append(const Metadata& md) = 0;
    virtual void flush(bool with_metadata) = 0;

    /**
     * Create an archive output for the given format and output file.
     *
     * format is a string corresponding to the output formats supported by
     * libarchive.
     */
    static std::unique_ptr<ArchiveOutput> create_file(const std::string& format, std::shared_ptr<core::NamedFileDescriptor> out);

    /**
     * Create an archive output for the given format and StreamOutput.
     *
     * format is a string corresponding to the output formats supported by
     * libarchive.
     */
    static std::unique_ptr<ArchiveOutput> create_stream(const std::string& format, std::shared_ptr<StreamOutput> out);
};

}
}

#endif
