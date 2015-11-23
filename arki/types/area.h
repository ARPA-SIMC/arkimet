#ifndef ARKI_TYPES_AREA_H
#define ARKI_TYPES_AREA_H

/*
 * types/area - Geographical area
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <memory>

#include <arki/types.h>
#include <arki/values.h>
#include <arki/utils/geosfwd.h>

struct lua_State;

namespace arki {
namespace types {

struct Area;

template<>
struct traits<Area>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The vertical area or layer of some data
 *
 * It can contain information like areatype and area value.
 */
struct Area : public types::StyledType<Area>
{
	mutable ARKI_GEOS_GEOMETRY* cached_bbox;

	/// Style values
	static const Style GRIB = 1;
	static const Style ODIMH5 = 2;
    static const Style VM2 = 3;

	Area();

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Area> decode(const unsigned char* buf, size_t len);
    static std::unique_ptr<Area> decodeString(const std::string& val);
    static std::unique_ptr<Area> decodeMapping(const emitter::memory::Mapping& val);

	/// Return the geographical bounding box
	const ARKI_GEOS_GEOMETRY* bbox() const;

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Area> createGRIB(const ValueBag& values);
    static std::unique_ptr<Area> createODIMH5(const ValueBag& values);
    static std::unique_ptr<Area> createVM2(unsigned station_id);
};

namespace area {

class GRIB : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~GRIB();

	const ValueBag& values() const { return m_values; }

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    GRIB* clone() const override;
    static std::unique_ptr<GRIB> create(const ValueBag& values);
    static std::unique_ptr<GRIB> decodeMapping(const emitter::memory::Mapping& val);
};

class ODIMH5 : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~ODIMH5();

	const ValueBag& values() const { return m_values; }

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    ODIMH5* clone() const override;
    static std::unique_ptr<ODIMH5> create(const ValueBag& values);
    static std::unique_ptr<ODIMH5> decodeMapping(const emitter::memory::Mapping& val);
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
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Area& o) const override;
    bool equals(const Type& o) const override;

    VM2* clone() const override;
    static std::unique_ptr<VM2> create(unsigned station_id);
    static std::unique_ptr<VM2> decodeMapping(const emitter::memory::Mapping& val);
};


}

}
}

// vim:set ts=4 sw=4:
#endif
