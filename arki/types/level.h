#ifndef ARKI_TYPES_LEVEL_H
#define ARKI_TYPES_LEVEL_H

#include <arki/types.h>
#include <stdint.h>

struct lua_State;

namespace arki {

namespace types {

struct Level;

template<>
struct traits<Level>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The vertical level or layer of some data
 *
 * It can contain information like leveltype and level value.
 */
struct Level : public types::StyledType<Level>
{
	/// Style values
	static const Style GRIB1 = 1;
	static const Style GRIB2S = 2;
	static const Style GRIB2D = 3;
	static const Style ODIMH5 = 4;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Level> decode(BinaryDecoder& dec);
    static std::unique_ptr<Level> decodeString(const std::string& val);
    static std::unique_ptr<Level> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Level> createGRIB1(unsigned char type);
    static std::unique_ptr<Level> createGRIB1(unsigned char type, unsigned short l1);
    static std::unique_ptr<Level> createGRIB1(unsigned char type, unsigned char l1, unsigned char l2);
    static std::unique_ptr<Level> createGRIB2S(uint8_t type, uint8_t scale, uint32_t val);
    static std::unique_ptr<Level> createGRIB2D(uint8_t type1, uint8_t scale1, uint32_t val1,
                                             uint8_t type2, uint8_t scale2, uint32_t val2);
    static std::unique_ptr<Level> createODIMH5(double value);
    static std::unique_ptr<Level> createODIMH5(double min, double max);
};

namespace level {

class GRIB1 : public Level
{
protected:
	unsigned char m_type;
	unsigned short m_l1;
	unsigned char m_l2;

public:
	unsigned type() const { return m_type; }
	unsigned l1() const { return m_l1; }
	unsigned l2() const { return m_l2; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    /**
     * Get information on how l1 and l2 should be treated:
     *
     * \l 0 means 'l1 and l2 should be ignored'
     * \l 1 means 'level, only l1 is to be considered'
     * \l 2 means 'layer from l1 to l2'
     */
    int valType() const;

    int compare_local(const Level& o) const override;
    bool equals(const Type& o) const override;

    GRIB1* clone() const override;
    static std::unique_ptr<GRIB1> create(unsigned char type);
    static std::unique_ptr<GRIB1> create(unsigned char type, unsigned short l1);
    static std::unique_ptr<GRIB1> create(unsigned char type, unsigned char l1, unsigned char l2);
    static std::unique_ptr<GRIB1> decodeMapping(const emitter::memory::Mapping& val);

    static int getValType(unsigned char type);
};

struct GRIB2 : public Level
{
	/**
	 * Get information on how l1 and l2 should be treated:
	 *
	 * \l 0 means 'l1 and l2 should be ignored'
	 * \l 1 means 'level, only l1 is to be considered'
	 * \l 2 means 'layer from l1 to l2'
	 */
	//int valType() const;
};

class GRIB2S : public GRIB2
{
protected:
	uint8_t m_type;
	uint8_t m_scale;
	uint32_t m_value;

public:
    static const uint8_t MISSING_TYPE;
    static const uint8_t MISSING_SCALE;
    static const uint32_t MISSING_VALUE;

    uint8_t type() const { return m_type; }
    uint8_t scale() const { return m_scale; }
    uint32_t value() const { return m_value; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Level& o) const override;
    bool equals(const Type& o) const override;

    GRIB2S* clone() const override;
    static std::unique_ptr<GRIB2S> create(uint8_t type, uint8_t scale, uint32_t val);
    static std::unique_ptr<GRIB2S> decodeMapping(const emitter::memory::Mapping& val);
};

class GRIB2D : public GRIB2
{
protected:
	uint8_t m_type1;
	uint8_t  m_scale1;
	uint32_t m_value1;
	uint8_t m_type2;
	uint8_t  m_scale2;
	uint32_t m_value2;

public:
	uint8_t type1() const { return m_type1; }
	uint8_t scale1() const { return m_scale1; }
	uint32_t value1() const { return m_value1; }
	uint8_t type2() const { return m_type2; }
	uint8_t scale2() const { return m_scale2; }
	uint32_t value2() const { return m_value2; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Level& o) const override;
    bool equals(const Type& o) const override;

    GRIB2D* clone() const override;
    static std::unique_ptr<GRIB2D> create(uint8_t type1, uint8_t scale1, uint32_t val1,
                               uint8_t type2, uint8_t scale2, uint32_t val2);

    static std::unique_ptr<GRIB2D> decodeMapping(const emitter::memory::Mapping& val);
};

class ODIMH5 : public Level
{
protected:
	double m_max;
	double m_min;

public:
	double max() const { return m_max; }
	double min() const { return m_min; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Level& o) const override;
    bool equals(const Type& o) const override;

    ODIMH5* clone() const override;
    static std::unique_ptr<ODIMH5> create(double value);
    static std::unique_ptr<ODIMH5> create(double min, double max);
    static std::unique_ptr<ODIMH5> decodeMapping(const emitter::memory::Mapping& val);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
