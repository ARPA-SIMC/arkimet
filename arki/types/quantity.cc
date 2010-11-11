/*
 * types/quantity - Metadata quantity
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/quantity.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE 		types::TYPE_QUANTITY
#define TAG 		"quantity"
#define SERSIZELEN 	1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki { namespace types {

/*============================================================================*/

const char* 		traits<Quantity>::type_tag 		= TAG;
const types::Code 	traits<Quantity>::type_code 		= CODE;
const size_t 		traits<Quantity>::type_sersize_bytes 	= SERSIZELEN;
const char* 		traits<Quantity>::type_lua_tag 		= LUATAG_TYPES ".quantity";

/*============================================================================*/

int Quantity::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Quantity* v = dynamic_cast<const Quantity*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Quantity::compare(const Quantity& o) const
{
	std::ostringstream ss1;
	std::ostringstream ss2;

	writeToOstream(ss1);
	o.writeToOstream(ss2);

	return ss1.str().compare(ss2.str());
	//	bool samevalues = true;
	//
	//	//guardo se tutti i valori miei ci sono in O
	//	for (std::set<std::string>::iterator i=values.begin(); i!=values.end(); i++)
	//	{
	//		if (o.values.find(*i) == o.values.end())
	//			return -1;
	//	}
	//
	//	//se sono qui ci sono tutti
	//	return values.size() - o.values.size();
}

bool Quantity::operator==(const Type& o) const
{
	const Quantity* v = dynamic_cast<const Quantity*>(&o);
	if (!v) return false;

	return compare(*v) == 0;
}

void Quantity::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addVarint(values.size());

	for (std::set<std::string>::iterator i=values.begin(); i!=values.end(); i++)
	{
		enc.addVarint((*i).size());
		enc.addString((*i));
	}
}

Item<Quantity> Quantity::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;

	ensureSize(len, 1, "quantity");
	Decoder dec(buf, len);
	size_t num 	= dec.popVarint<size_t>("quantity num elemetns");

	std::set<std::string> vals;

	for (size_t i=0; i<num; i++)
	{
		size_t vallen 	= dec.popVarint<size_t>("quantity name len");
		string val 	= dec.popString(vallen, "quantity name");
		vals.insert(val);
	}

	return Quantity::create(vals);
}

std::ostream& Quantity::writeToOstream(std::ostream& o) const
{
	return o << str::join(values.begin(), values.end(), ", ");
}

void Quantity::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("va");
    e.start_list();
    for (set<string>::const_iterator i = values.begin();
            i != values.end(); ++i)
        e.add(*i);
    e.end_list();
}

Item<Quantity> Quantity::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    const List& l = val["va"].want_list("parsing Quantity values");
    set<string> vals;
    for (vector<const Node*>::const_iterator i = l.val.begin();
            i != l.val.end(); ++i)
        vals.insert((*i)->want_string("parsing a Quantity value"));
    return Quantity::create(vals);
}

Item<Quantity> Quantity::decodeString(const std::string& val)
{
	if (val.empty())
		throw wibble::exception::Consistency("parsing Quantity", "string is empty");

	std::set<std::string> vals;
	split(val, vals);
	return Quantity::create(vals);
}

#ifdef HAVE_LUA
bool Quantity::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "quantity")
	{
		//TODO XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		//lua_pushlstring(L, task.data(), task.size());
	}
	else
		return CoreType<Quantity>::lua_lookup(L, name);
	return true;
}

static int arkilua_new_quantity(lua_State* L)
{
	set<string> values;
	if (lua_gettop(L) == 1 && lua_istable(L, 1))
	{
		// Iterate the table building the values vector

		lua_pushnil(L); // first key
		while (lua_next(L, 1) != 0) {
			// Ignore index at -2
			const char* val = lua_tostring(L, -1);
			values.insert(val);
			// removes 'value'; keeps 'key' for next iteration
			lua_pop(L, 1);
		}
	} else {
		unsigned count = lua_gettop(L);
		for (unsigned i = 0; i != count; ++i)
			values.insert(lua_tostring(L, i + 1));
	}
	
	Quantity::create(values)->lua_push(L);
	return 1;
}

void Quantity::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "new", arkilua_new_quantity },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_quantity", lib, 0);
	lua_pop(L, 1);
}
#endif

Item<Quantity> Quantity::create(const std::string& values)
{
	std::set<std::string> vals;
	split(values, vals);
	return Quantity::create(vals);
}

Item<Quantity> Quantity::create(const std::set<std::string>& values)
{
	return new Quantity(values);
}


/*============================================================================*/

static MetadataType quantityType = MetadataType::create<Quantity>();

/*============================================================================*/

} }

#include <arki/types.tcc>
// vim:set ts=4 sw=4:




























