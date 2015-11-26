/*
 * types/value - Metadata type to store small values
 *
 * Copyright (C) 2012--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/wibble/exception.h>
#include <arki/wibble/string.h>
#include <arki/wibble/regexp.h>
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
#define SERSIZELEN 0   // Not supported in version 1

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

bool Value::equals(const Type& o) const
{
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v) return false;
    return buffer == v->buffer;
}

int Value::compare(const Type& o) const
{
    using namespace wibble;

    int res = CoreType<Value>::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v)
        throw wibble::exception::Consistency(
                "comparing metadata types",
                str::fmtf("second element claims to be `value', but is `%s' instead",
                    typeid(&o).name()));

    // Just compare buffers
    if (buffer < v->buffer) return -1;
    if (buffer == v->buffer) return 0;
    return 1;
}

void Value::encodeWithoutEnvelope(Encoder& enc) const
{
    enc.addString(buffer);
}

std::ostream& Value::writeToOstream(std::ostream& o) const
{
    return o << str::c_escape(buffer);
}

void Value::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("va", buffer);
}

unique_ptr<Value> Value::decode(const unsigned char* buf, size_t len)
{
    return Value::create(string((const char*)buf, len));
}

unique_ptr<Value> Value::decodeString(const std::string& val)
{
    size_t len;
    return Value::create(str::c_unescape(val, len));
}

unique_ptr<Value> Value::decodeMapping(const emitter::memory::Mapping& val)
{
    return Value::create(val["va"].want_string("parsing item value encoded in metadata"));
}

Value* Value::clone() const
{
    Value* val = new Value;
    val->buffer = buffer;
    return val;
}

unique_ptr<Value> Value::create(const std::string& buf)
{
    Value* val = new Value;
    val->buffer = buf;
    return unique_ptr<Value>(val);
}

void Value::lua_loadlib(lua_State* L)
{
#if 0
	static const struct luaL_Reg lib [] = {
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
