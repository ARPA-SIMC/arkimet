#ifndef ARKI_TYPES_SOURCE_H
#define ARKI_TYPES_SOURCE_H

/// Represent where the data for a metadata can be found

#include <arki/libconfig.h>
#include <arki/types/styled.h>
#include <arki/segment/fwd.h>
#include <stddef.h>
#include <stdint.h>

struct lua_State;

namespace arki {
namespace types {
namespace source {

/// Style values
enum class Style: unsigned char {
    BLOB = 1,
    URL = 2,
    INLINE = 3,
};

}

template<>
struct traits<Source>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

    /// Style values
    typedef source::Style Style;
};

/**
 * The place where the data is stored
 */
struct Source : public types::StyledType<Source>
{
    std::string format;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	virtual int compare_local(const Source& o) const;

    /// CODEC functions
    virtual void encodeWithoutEnvelope(BinaryEncoder& enc) const;
    static std::unique_ptr<Source> decode(BinaryDecoder& dec);
    static std::unique_ptr<Source> decodeRelative(BinaryDecoder& dec, const std::string& basedir);
    static std::unique_ptr<Source> decodeString(const std::string& val);
    static std::unique_ptr<Source> decode_structure(const structured::Keys& keys, const structured::Reader& val);
    virtual void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const;

    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    Source* clone() const = 0;

    // Register this type with the type system
    static void init();

    static std::unique_ptr<Source> createBlob(std::shared_ptr<segment::Reader> reader, uint64_t offset, uint64_t size);
    static std::unique_ptr<Source> createBlob(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size, std::shared_ptr<segment::Reader> reader);
    static std::unique_ptr<Source> createBlobUnlocked(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static std::unique_ptr<Source> createInline(const std::string& format, uint64_t size);
    static std::unique_ptr<Source> createURL(const std::string& format, const std::string& url);
};

namespace source {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Source::formatStyle(s); }

}

}
}
#endif
