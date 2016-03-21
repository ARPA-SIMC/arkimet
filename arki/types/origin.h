#ifndef ARKI_TYPES_ORIGIN_H
#define ARKI_TYPES_ORIGIN_H

#include <arki/types.h>

struct lua_State;

namespace arki {

namespace types {

struct Origin;

template<>
struct traits<Origin>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The origin of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Origin : public types::StyledType<Origin>
{
	/// Style values
	//static const Style NONE = 0;
	static const Style GRIB1 = 1;
	static const Style GRIB2 = 2;
	static const Style BUFR = 3;
	static const Style ODIMH5 = 4;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Origin> decode(BinaryDecoder& dec);
    static std::unique_ptr<Origin> decodeString(const std::string& val);
    static std::unique_ptr<Origin> decodeMapping(const emitter::memory::Mapping& val);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const = 0;
	static int getMaxIntCount();

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Origin> createGRIB1(unsigned char centre, unsigned char subcentre, unsigned char process);
    static std::unique_ptr<Origin> createGRIB2(unsigned short centre, unsigned short subcentre,
                                             unsigned char processtype, unsigned char bgprocessid, unsigned char processid);
    static std::unique_ptr<Origin> createBUFR(unsigned char centre, unsigned char subcentre);
    static std::unique_ptr<Origin> createODIMH5(const std::string& wmo, const std::string& rad, const std::string& plc);
};

namespace origin {

class GRIB1 : public Origin
{
protected:
	unsigned char m_centre;
	unsigned char m_subcentre;
	unsigned char m_process;

public:
	virtual ~GRIB1();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }
	unsigned process() const { return m_process; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Origin& o) const override;
    bool equals(const Type& o) const override;

    GRIB1* clone() const override;
    static std::unique_ptr<GRIB1> create(unsigned char centre, unsigned char subcentre, unsigned char process);
    static std::unique_ptr<GRIB1> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class GRIB2 : public Origin
{
protected:
	unsigned short m_centre;
	unsigned short m_subcentre;
	unsigned char m_processtype;
	unsigned char m_bgprocessid;
	unsigned char m_processid;

public:
	virtual ~GRIB2();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }
	unsigned processtype() const { return m_processtype; }
	unsigned bgprocessid() const { return m_bgprocessid; }
	unsigned processid() const { return m_processid; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Origin& o) const override;
    bool equals(const Type& o) const override;

    GRIB2* clone() const override;
    static std::unique_ptr<GRIB2> create(unsigned short centre, unsigned short subcentre,
            unsigned char processtype, unsigned char bgprocessid, unsigned char processid);
    static std::unique_ptr<GRIB2> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class BUFR : public Origin
{
protected:
	unsigned char m_centre;
	unsigned char m_subcentre;

public:
	virtual ~BUFR();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Origin& o) const override;
    bool equals(const Type& o) const override;

    BUFR* clone() const override;
    static std::unique_ptr<BUFR> create(unsigned char centre, unsigned char subcentre);
    static std::unique_ptr<BUFR> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class ODIMH5 : public Origin
{
protected:
	std::string m_WMO;
	std::string m_RAD;
	std::string m_PLC;

public:
	virtual ~ODIMH5();

	const std::string& getWMO() const { return m_WMO; }
	const std::string& getRAD() const { return m_RAD; }
	const std::string& getPLC() const { return m_PLC; }

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Origin& o) const override;
    bool equals(const Type& o) const override;

    ODIMH5* clone() const override;
    static std::unique_ptr<ODIMH5> create(const std::string& wmo, const std::string& rad, const std::string& plc);
    static std::unique_ptr<ODIMH5> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

}

}

}

// vim:set ts=4 sw=4:
#endif
