#ifndef ARKI_TYPES_PRODDEF_H
#define ARKI_TYPES_PRODDEF_H

/*
 * types/proddef - Product definition
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

#include <arki/types.h>
#include <arki/values.h>

struct lua_State;

namespace arki {
namespace types {

struct Proddef;

template<>
struct traits<Proddef>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The vertical proddef or layer of some data
 *
 * It can contain information like proddef type and proddef value.
 */
struct Proddef : public types::StyledType<Proddef>
{
	/// Style values
	static const Style GRIB = 1;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::auto_ptr<Proddef> decode(const unsigned char* buf, size_t len);
    static std::auto_ptr<Proddef> decodeString(const std::string& val);
    static std::auto_ptr<Proddef> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();

    static std::auto_ptr<Proddef> createGRIB(const ValueBag& values);
};

namespace proddef {

struct GRIB : public Proddef
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

    int compare_local(const Proddef& o) const override;
    bool equals(const Type& o) const override;

    GRIB* clone() const override;
    static std::auto_ptr<GRIB> create(const ValueBag& values);
    static std::auto_ptr<GRIB> decodeMapping(const emitter::memory::Mapping& val);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
