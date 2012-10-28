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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <arki/types/value.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_VALUE
#define TAG "value"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

const char* traits<Value>::type_tag = TAG;
const types::Code traits<Value>::type_code = CODE;
const size_t traits<Value>::type_sersize_bytes = SERSIZELEN;
const char* traits<Value>::type_lua_tag = LUATAG_TYPES ".run";

bool Value::operator==(const Type& o) const
{
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v) return false;
    return buffer == v->buffer;
}

void Value::encodeWithoutEnvelope(Encoder& enc) const
{
    enc.addVarint(buffer.size());
    enc.addString(buffer);
}

std::ostream& Value::writeToOstream(std::ostream& o) const
{
#if 0
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed
			 << setw(2) << (m_minute / 60) << ":"
			 << setw(2) << (m_minute % 60) << ")";
#endif
    return o;
}

void Value::serialiseLocal(Emitter& e, const Formatter* f) const
{
    //e.add("va", (int)m_minute);
}

Item<Value> Value::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
#if 0
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "run style");
	switch (s)
	{
		case MINUTE: {
		        unsigned int m = dec.popVarint<unsigned>("run minute");
			return run::Minute::create(m / 60, m % 60);
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "style is " + formatStyle(s) + " but we can only decode MINUTE");
	}
#endif
    return Value::create("");
}

Item<Value> Value::decodeString(const std::string& val)
{
#if 0
	string inner;
	Run::Style style = outerParse<Run>(val, inner);
	switch (style)
	{
		case Run::MINUTE: {
			size_t sep = inner.find(':');
			int hour, minute;
			if (sep == string::npos)
			{
				// 12
				hour = strtoul(inner.c_str(), 0, 10);
				minute = 0;
			} else {
				// 12:00
				hour = strtoul(inner.substr(0, sep).c_str(), 0, 10);
				minute = strtoul(inner.substr(sep+1).c_str(), 0, 10);
			}
			return run::Minute::create(hour, minute);
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "unknown Run style " + formatStyle(style));
	}
#endif
    return Value::create("");
}

Item<Value> Value::decodeMapping(const emitter::memory::Mapping& val)
{
#if 0
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Run::MINUTE: return run::Minute::decodeMapping(val);
        default:
            throw wibble::exception::Consistency("parsing Run", "unknown Run style " + val.get_string());
    }
#endif
    return Value::create("");
}

Item<Value> Value::create(const std::string& buf)
{
    Value* val;
    Item<Value> res = val = new Value;
    val->buffer = buf;
    return res;;
}

#if 0
static int arkilua_new_minute(lua_State* L)
{
	int nargs = lua_gettop(L);
	int hour = luaL_checkint(L, 1);
	if (nargs == 1)
	{
		run::Minute::create(hour, 0)->lua_push(L);
	} else {
		int minute = luaL_checkint(L, 2);
		run::Minute::create(hour, minute)->lua_push(L);
	}
	return 1;
    return 0;
}
#endif

void Value::lua_loadlib(lua_State* L)
{
#if 0
	static const struct luaL_reg lib [] = {
		{ "minute", arkilua_new_minute },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_run", lib, 0);
	lua_pop(L, 1);
#endif
}

void Value::init()
{
    MetadataType::register_type<Value>();
}

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
