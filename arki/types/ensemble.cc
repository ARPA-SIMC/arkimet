/*
 * types/ensemble - Geographical ensemble
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/ensemble.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_ENSEMBLE
#define TAG "ensemble"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

// Style constants
const unsigned char Ensemble::GRIB;

Ensemble::Style Ensemble::parseStyle(const std::string& str)
{
	if (str == "GRIB") return GRIB;
	throw wibble::exception::Consistency("parsing Ensemble style", "cannot parse Ensemble style '"+str+"': only GRIB is supported");
}

std::string Ensemble::formatStyle(Ensemble::Style s)
{
	switch (s)
	{
		case Ensemble::GRIB: return "GRIB";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Ensemble::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Ensemble* v = dynamic_cast<const Ensemble*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Ensemble, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Ensemble::compare(const Ensemble& o) const
{
	return style() - o.style();
}

types::Code Ensemble::serialisationCode() const { return CODE; }
size_t Ensemble::serialisationSizeLength() const { return SERSIZELEN; }
std::string Ensemble::tag() const { return TAG; }

void Ensemble::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
}

Item<Ensemble> Ensemble::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Ensemble");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case GRIB:
			return ensemble::GRIB::create(ValueBag::decode(buf+1, len-1));
		default:
			throw wibble::exception::Consistency("parsing Ensemble", "style is " + formatStyle(s) + " but we can only decode GRIB");
	}
}

Item<Ensemble> Ensemble::decodeString(const std::string& val)
{
	string inner;
	Ensemble::Style style = outerParse<Ensemble>(val, inner);
	switch (style)
	{
		case Ensemble::GRIB: return ensemble::GRIB::create(ValueBag::parse(inner)); 
		default:
			throw wibble::exception::Consistency("parsing Ensemble", "unknown Ensemble style " + formatStyle(style));
	}
}

//////////////////////////////
#ifdef HAVE_LUA
int Ensemble::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Ensemble reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Ensemble& v = **(const Ensemble**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Ensemble::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "grib" && v.style() == Ensemble::GRIB)
	{
		const ensemble::GRIB* v1 = v.upcast<ensemble::GRIB>();
		v1->values.lua_push(L);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_ensemble(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Ensemble::lua_lookup, 2);
	return 1;
}
void Ensemble::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Ensemble** s = (const Ensemble**)lua_newuserdata(L, sizeof(const Ensemble*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_ensemble);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Ensemble>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace ensemble {

Ensemble::Style GRIB::style() const { return Ensemble::GRIB; }

void GRIB::encodeWithoutEnvelope(Encoder& enc) const
{
	Ensemble::encodeWithoutEnvelope(enc);
	values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << values.toString() << ")";
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + values.toString();
}

int GRIB::compare(const Ensemble& o) const
{
	if (int res = Ensemble::compare(o)) return res;

	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB Ensemble, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB::compare(const GRIB& o) const
{
	return values.compare(o.values);
}

bool GRIB::operator==(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return values == v->values;
}

Item<GRIB> GRIB::create(const ValueBag& values)
{
	GRIB* res = new GRIB;
	res->values = values;
	return res;
}

}

static MetadataType ensembleType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Ensemble::decode),
	(MetadataType::string_decoder)(&Ensemble::decodeString));

}
}
// vim:set ts=4 sw=4:


#if 0
	-- Move to the matcher
	virtual bool match(const ValueBag& wanted) const
	{
		// Both a and b are sorted, so we can iterate them linearly together

		values_t::const_iterator a = values.begin();
		ValueBag::const_iterator b = wanted.begin();

		while (a != values.end())
		{
			// Nothing else wanted anymore
			if (b == wanted.end())
				return true;
			if (a->first < b->first)
				// This value is not in the match expression
				++a;
			else if (b->first < a->first)
				// This value is wanted but we don't have it
				return false;
			else if (*a->second != *b->second)
				// Same key, check if the value is the same
				return false;
			else
			{
				// If also the value is the same, move on to the next item
				++a;
				++b;
			}
		}
		// We got to the end of a.  If there are still things in b, we don't
		// match.  If we are also to the end of b, then we matched everything
		return b == wanted.end();
	}
#endif
