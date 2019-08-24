#ifndef ARKI_TYPES_AREA_H
#define ARKI_TYPES_AREA_H

#include <memory>
#include <arki/types/styled.h>
#include <arki/values.h>
#include <arki/utils/geosfwd.h>

struct lua_State;

namespace arki {
namespace types {

namespace area {

/// Style values
enum class Style: unsigned char {
    GRIB = 1,
    ODIMH5 = 2,
    VM2 = 3,
};

}

template<>
struct traits<Area>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    static const char* type_lua_tag;
    typedef area::Style Style;
};

/**
 * The vertical area or layer of some data
 *
 * It can contain information like areatype and area value.
 */
struct Area : public types::StyledType<Area>
{
    mutable arki::utils::geos::Geometry* cached_bbox = nullptr;

	Area();

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Area> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Area> decodeString(const std::string& val);
    static std::unique_ptr<Area> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    /// Return the geographical bounding box
    const arki::utils::geos::Geometry* bbox() const;

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Area> createGRIB(const ValueBag& values);
    static std::unique_ptr<Area> createODIMH5(const ValueBag& values);
    static std::unique_ptr<Area> createVM2(unsigned station_id);
};

namespace area {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Area::formatStyle(s); }


class GRIB : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~GRIB();

	const ValueBag& values() const { return m_values; }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    GRIB* clone() const override;
    static std::unique_ptr<GRIB> create(const ValueBag& values);
    static std::unique_ptr<GRIB> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class ODIMH5 : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~ODIMH5();

	const ValueBag& values() const { return m_values; }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    ODIMH5* clone() const override;
    static std::unique_ptr<ODIMH5> create(const ValueBag& values);
    static std::unique_ptr<ODIMH5> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class VM2 : public Area
{
protected:
    unsigned m_station_id;
    mutable std::unique_ptr<ValueBag> m_derived_values;

public:
    virtual ~VM2();

    unsigned station_id() const { return m_station_id; }
    const ValueBag& derived_values() const;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    void encode_for_indexing(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    VM2* clone() const override;
    static std::unique_ptr<VM2> create(unsigned station_id);
    static std::unique_ptr<VM2> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};


}
}
}
#endif
