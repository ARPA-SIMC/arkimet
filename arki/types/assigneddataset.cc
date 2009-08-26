/*
 * types/assigneddataset - Metadata annotation
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
#include <arki/types/assigneddataset.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_ASSIGNEDDATASET
#define TAG "assigneddataset"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

int AssignedDataset::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a AssignedDataset, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int AssignedDataset::compare(const AssignedDataset& o) const
{
	if (name < o.name) return -1;
	if (name > o.name) return 1;
	if (id < o.id) return -1;
	if (id > o.id) return 1;
	return 0;
}

bool AssignedDataset::operator==(const Type& o) const
{
	const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
	if (!v) return false;
	return name == v->name && id == v->id;
}

types::Code AssignedDataset::serialisationCode() const { return CODE; }
size_t AssignedDataset::serialisationSizeLength() const { return SERSIZELEN; }
std::string AssignedDataset::tag() const { return TAG; }

void AssignedDataset::encodeWithoutEnvelope(Encoder& enc) const
{
	changed->encodeWithoutEnvelope(enc);
	enc.addUInt(name.size(), 1).addString(name).addUInt(id.size(), 2).addString(id);
}

//////////////////////////////////////

Item<AssignedDataset> AssignedDataset::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 8, "AssignedDataset");
	Item<Time> changed = Time::decode(buf, 5);
	size_t name_len = decodeUInt(buf+5, 1);
	size_t id_len = decodeUInt(buf+5+1+name_len, 2);
	ensureSize(len, 8 + name_len + id_len, "AssignedDataset");
	string name = string((char*)buf+5+1, 0, name_len);
	string id = string((char*)buf+5+1+name_len+2, 0, id_len);
	return AssignedDataset::create(changed, name, id);
}

std::ostream& AssignedDataset::writeToOstream(std::ostream& o) const
{
	return o << name << " as " << id << " imported on " << changed;
}

Item<AssignedDataset> AssignedDataset::decodeString(const std::string& val)
{
	size_t pos = val.find(" as ");
	if (pos == string::npos)
		throw wibble::exception::Consistency("parsing dataset attribution",
			"string \"" + val + "\" does not contain \" as \"");
	string name = val.substr(0, pos);
	pos += 4;
	size_t idpos = val.find(" imported on ", pos);
	if (idpos == string::npos)
		throw wibble::exception::Consistency("parsing dataset attribution",
			"string \"" + val + "\" does not contain \" imported on \" after the dataset name");
	string id = val.substr(pos, idpos - pos);
	pos = idpos + 13;
	Item<Time> changed = Time::decodeString(val.substr(pos));

	return AssignedDataset::create(changed, name, id);
}

#ifdef HAVE_LUA
int AssignedDataset::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the AssignedDataset reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const AssignedDataset& v = **(const AssignedDataset**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "changed")
	{
		v.changed->lua_push(L);
		return 1;
	}
	else if (name == "name")
	{
		lua_pushlstring(L, v.name.data(), v.name.size());
		return 1;
	}
	else if (name == "id")
	{
		lua_pushlstring(L, v.id.data(), v.id.size());
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_assigneddataset(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, AssignedDataset::lua_lookup, 2);
	return 1;
}
void AssignedDataset::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const AssignedDataset** s = (const AssignedDataset**)lua_newuserdata(L, sizeof(const AssignedDataset*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_assigneddataset);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<AssignedDataset>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

Item<AssignedDataset> AssignedDataset::create(const std::string& name, const std::string& id)
{
	return new AssignedDataset(Time::createNow(), name, id);
}

Item<AssignedDataset> AssignedDataset::create(const Item<types::Time>& changed, const std::string& name, const std::string& id)
{
	return new AssignedDataset(changed, name, id);
}

static MetadataType assigneddatasetType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&AssignedDataset::decode),
	(MetadataType::string_decoder)(&AssignedDataset::decodeString));

}
}
// vim:set ts=4 sw=4:
