#ifndef ARKI_TYPES_PRODDEF_H
#define ARKI_TYPES_PRODDEF_H

#include <arki/types/styled.h>
#include <arki/values.h>

struct lua_State;

namespace arki {
namespace types {

namespace proddef {

/// Style values
enum class Style: unsigned char {
    GRIB = 1,
};

}

struct Proddef;

template<>
struct traits<Proddef>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

    typedef proddef::Style Style;
};

/**
 * The vertical proddef or layer of some data
 *
 * It can contain information like proddef type and proddef value.
 */
struct Proddef : public types::StyledType<Proddef>
{
	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Proddef> decode(BinaryDecoder& dec);
    static std::unique_ptr<Proddef> decodeString(const std::string& val);
    static std::unique_ptr<Proddef> decodeMapping(const emitter::memory::Mapping& val);
    static std::unique_ptr<Proddef> decode_structure(const emitter::Keys& keys, const emitter::Reader& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Proddef> createGRIB(const ValueBag& values);
};

namespace proddef {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Proddef::formatStyle(s); }


struct GRIB : public Proddef
{
protected:
	ValueBag m_values;

public:
	virtual ~GRIB();

	const ValueBag& values() const { return m_values; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Proddef& o) const override;
    bool equals(const Type& o) const override;

    GRIB* clone() const override;
    static std::unique_ptr<GRIB> create(const ValueBag& values);
    static std::unique_ptr<GRIB> decodeMapping(const emitter::memory::Mapping& val);
    static std::unique_ptr<GRIB> decode_structure(const emitter::Keys& keys, const emitter::Reader& val);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
