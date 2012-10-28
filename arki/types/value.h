#ifndef ARKI_TYPES_VALUE_H
#define ARKI_TYPES_VALUE_H

/*
 * types/value - Metadata type to store small values
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <string>

struct lua_State;

namespace arki {
namespace types {

struct Value;

template<>
struct traits<Value>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The run of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Value : public types::CoreType<Value>
{
    std::string buffer;

    virtual bool operator==(const Type& o) const;
    virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
    virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;

    /// CODEC functions
    static Item<Value> decode(const unsigned char* buf, size_t len);
    static Item<Value> decodeString(const std::string& val);
    static Item<Value> decodeMapping(const emitter::memory::Mapping& val);

    static Item<Value> create(const std::string& buf);

    static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

}
}

// vim:set ts=4 sw=4:
#endif
