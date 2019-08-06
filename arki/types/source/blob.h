#ifndef ARKI_TYPES_SOURCE_BLOB_H
#define ARKI_TYPES_SOURCE_BLOB_H

#include <arki/types/source.h>
#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>

namespace arki {
namespace types {
namespace source {

struct Blob : public Source
{
    /**
     * Base directory used to resolve relative filenames.
     *
     * Note that this is not stored when serializing, since metadata usually
     * point to files relative to the metadata location, in order to save
     * space.
     */
    std::string basedir;

    /**
     * Data file name.
     *
     * Can be an absolute or a relative path. If it is a relative path, it is
     * resolved based on \a basedir, or on the current directory if \a basedir
     * is empty.
     */
    std::string filename;

    uint64_t offset;
    uint64_t size;

    /**
     * Reader used to load the data pointed by this blob.
     *
     * The reader will keep a read lock on the file, to prevent it from being
     * modified while it can still potentially be read
     */
    std::shared_ptr<segment::Reader> reader;


    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;

    Blob* clone() const override;

    /// Return the absolute pathname to the data file
    std::string absolutePathname() const;

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
    std::unique_ptr<Blob> makeRelativeTo(const std::string& path) const;

    /**
     * Make sure this blob has a reader that keeps a read lock on the source file
     */
    void lock(std::shared_ptr<segment::Reader> reader);

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
    size_t stream_data(core::NamedFileDescriptor& out) const;
    size_t stream_data(core::AbstractOutputFile& out) const;

    static std::unique_ptr<Blob> create(std::shared_ptr<segment::Reader> reader, uint64_t offset, uint64_t size);
    static std::unique_ptr<Blob> create(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size, std::shared_ptr<segment::Reader> reader);
    static std::unique_ptr<Blob> create_unlocked(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static std::unique_ptr<Blob> decodeMapping(const emitter::memory::Mapping& val);
    static std::unique_ptr<Blob> decode_structure(const emitter::Keys& keys, const emitter::Reader& reader);
};

}
}
}
#endif
