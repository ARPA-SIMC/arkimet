#ifndef ARKI_TYPES_SOURCE_BLOB_H
#define ARKI_TYPES_SOURCE_BLOB_H

#include <arki/types/source.h>

namespace arki {

namespace utils {
struct DataReader;
}

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

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
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

    static std::unique_ptr<Blob> create(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static std::unique_ptr<Blob> decodeMapping(const emitter::memory::Mapping& val);
};

}
}
}
#endif
