#ifndef ARKI_TYPES_SOURCE_BLOB_H
#define ARKI_TYPES_SOURCE_BLOB_H

#include <cstdint>
#include <filesystem>
#include <arki/types/source.h>
#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/segment/fwd.h>

namespace arki {
namespace types {
namespace source {

class Blob : public Source
{
public:
    constexpr static const char* name = "Blob";
    constexpr static const char* doc = R"(
The data is available in the local file system:

* ``filename`` points to the file that has the data
* ``offset`` is the position in the file where the data is stored
* ``size`` is the size in bytes of the data

``basedir`` is a hint that can be used to resolve relative ``filename`` values.
It is not stored in the metadata, and when reading it defaults to the path
where the metadata is found, so that data can be referenced relative to the
metadata.

It is possible that ``filename`` points to a directory segment or a ``.zip``
file: in that case, the value of the offset is used to reference the data in
the directory or zipfile based on the meaning given by the directory segment or
zip file segment implementation.
)";
    /**
     * Base directory used to resolve relative filenames.
     *
     * Note that this is not stored when serializing, since metadata usually
     * point to files relative to the metadata location, in order to save
     * space.
     */
    std::filesystem::path basedir;

    /**
     * Data file name.
     *
     * Can be an absolute or a relative path. If it is a relative path, it is
     * resolved based on \a basedir, or on the current directory if \a basedir
     * is empty.
     */
    std::filesystem::path filename;

    uint64_t offset;
    uint64_t size;

    /**
     * Reader used to load the data pointed by this blob.
     *
     * The reader will keep a read lock on the file, to prevent it from being
     * modified while it can still potentially be read
     */
    std::shared_ptr<segment::data::Reader> reader;


    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;

    Blob* clone() const override;

    /// Return the absolute pathname to the data file
    std::filesystem::path absolutePathname() const;

    /**
     * Return a new source identical to this one, but with all the
     * directory components stripped from the file name.
     *
     * basedir is updated so that we can still reach the data file.
     */
    std::unique_ptr<Blob> fileOnly() const;

    /**
     * Return a new source identical to this one, but with an absolute file
     * name and no basedir.
     */
    std::unique_ptr<Blob> makeAbsolute() const;

    /**
     * Return a new source identical to this one, but relative to path instead
     * of the current basedir.
     *
     * Throws an exception if path is not a directory that contains the current
     * absolute source pathname.
     */
    std::unique_ptr<Blob> makeRelativeTo(const std::filesystem::path& path) const;

    /**
     * Return a new source for storing in segment metadata.
     *
     * This relies on the invariant that the written metadata is stored next to
     * the data, so only offset and size are needed to locate the data.
     */
    std::unique_ptr<Blob> for_segment_metadata() const;

    /**
     * Make sure this blob has a reader that keeps a read lock on the source file
     */
    void lock(std::shared_ptr<segment::data::Reader> reader);

    /**
     * Make sure this blob is not holding a read lock on the source file
     */
    void unlock();

    /**
     * Get the data referred by this blob, read from the given file descriptor.
     * It is up to the caller to ensure that fd is open on the right file.
     *
     * If rlock is true, the file descriptor will be locked for reading during
     * I/O
     */
    std::vector<uint8_t> read_data(core::NamedFileDescriptor& fd, bool rlock=true) const;

    /**
     * Get the data referred by this blob via its reader.
     *
     * This only works on locked blobs.
     */
    std::vector<uint8_t> read_data() const;

    /**
     * Copy the data referred by this blob to the given file descriptor.
     *
     * This only works on locked blobs.
     *
     * Returns the number of bytes written to out.
     */
    stream::SendResult stream_data(StreamOutput& out) const;

    static std::unique_ptr<Blob> create(std::shared_ptr<segment::data::Reader> reader, uint64_t offset, uint64_t size);
    static std::unique_ptr<Blob> create(DataFormat format, const std::filesystem::path& basedir, const std::filesystem::path& filename, uint64_t offset, uint64_t size, std::shared_ptr<segment::data::Reader> reader);
    static std::unique_ptr<Blob> create_unlocked(DataFormat format, const std::filesystem::path& basedir, const std::filesystem::path& filename, uint64_t offset, uint64_t size);
    static std::unique_ptr<Blob> decode_structure(const structured::Keys& keys, const structured::Reader& reader);
};

}
}
}
#endif
