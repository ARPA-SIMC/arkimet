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
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
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

const char* traits<AssignedDataset>::type_tag = TAG;
const types::Code traits<AssignedDataset>::type_code = CODE;
const size_t traits<AssignedDataset>::type_sersize_bytes = SERSIZELEN;
const char* traits<AssignedDataset>::type_lua_tag = LUATAG_TYPES ".assigneddataset";

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

void AssignedDataset::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("ti"); changed->serialiseList(e);
    e.add("na", name);
    e.add("id", id);
}

Item<AssignedDataset> AssignedDataset::decodeMapping(const emitter::memory::Mapping& val)
{
    return AssignedDataset::create(
            Time::decodeList(val["ti"].want_list("parsing AssignedDataset time")),
            val["na"].want_string("parsing AssignedDataset name"),
            val["id"].want_string("parsing AssignedDataset id"));
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
bool AssignedDataset::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "changed")
		changed->lua_push(L);
	else if (name == "name")
		lua_pushlstring(L, this->name.data(), this->name.size());
	else if (name == "id")
		lua_pushlstring(L, id.data(), id.size());
	else
		return CoreType<AssignedDataset>::lua_lookup(L, name);
	return true;
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

static MetadataType assigneddatasetType = MetadataType::create<AssignedDataset>();

}
}

#include <arki/types.tcc>
// vim:set ts=4 sw=4:
