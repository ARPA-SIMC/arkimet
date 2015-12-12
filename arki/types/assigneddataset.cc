#include <arki/wibble/exception.h>
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

int AssignedDataset::compare(const Type& o) const
{
    if (int res = types::CoreType<AssignedDataset>::compare(o)) return res;
    const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
    if (name < v->name) return -1;
    if (name > v->name) return 1;
    if (id < v->id) return -1;
    if (id > v->id) return 1;
    return 0;
}

bool AssignedDataset::equals(const Type& o) const
{
	const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
	if (!v) return false;
	return name == v->name && id == v->id;
}

void AssignedDataset::encodeWithoutEnvelope(Encoder& enc) const
{
    changed.encodeWithoutEnvelope(enc);
    enc.addUInt(name.size(), 1); enc.addString(name); enc.addUInt(id.size(), 2); enc.addString(id);
}

//////////////////////////////////////

unique_ptr<AssignedDataset> AssignedDataset::decode(const unsigned char* buf, size_t len)
{
    using namespace utils::codec;
    ensureSize(len, 8, "AssignedDataset");
    unique_ptr<Time> changed = Time::decode(buf, 5);
    Decoder dec(buf + 5, len - 5);
    size_t name_len = dec.popUInt(1, "length of dataset name");
    string name = dec.popString(name_len, "dataset name");
    size_t id_len = dec.popUInt(2, "length of dataset id");
    string id = dec.popString(id_len, "dataset id");
    return AssignedDataset::create(*changed, name, id);
}

std::ostream& AssignedDataset::writeToOstream(std::ostream& o) const
{
	return o << name << " as " << id << " imported on " << changed;
}

void AssignedDataset::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("ti"); changed.serialiseList(e);
    e.add("na", name);
    e.add("id", id);
}

unique_ptr<AssignedDataset> AssignedDataset::decodeMapping(const emitter::memory::Mapping& val)
{
    return AssignedDataset::create(
            *Time::decodeList(val["ti"].want_list("parsing AssignedDataset time")),
            val["na"].want_string("parsing AssignedDataset name"),
            val["id"].want_string("parsing AssignedDataset id"));
}


unique_ptr<AssignedDataset> AssignedDataset::decodeString(const std::string& val)
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
    unique_ptr<Time> changed = Time::decodeString(val.substr(pos));

    return AssignedDataset::create(*changed, name, id);
}

#ifdef HAVE_LUA
bool AssignedDataset::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "changed")
        changed.lua_push(L);
    else if (name == "name")
        lua_pushlstring(L, this->name.data(), this->name.size());
    else if (name == "id")
        lua_pushlstring(L, id.data(), id.size());
    else
        return CoreType<AssignedDataset>::lua_lookup(L, name);
    return true;
}
#endif

AssignedDataset* AssignedDataset::clone() const
{
    return new AssignedDataset(changed, name, id);
}

unique_ptr<AssignedDataset> AssignedDataset::create(const std::string& name, const std::string& id)
{
    return unique_ptr<AssignedDataset>(new AssignedDataset(Time::createNow(), name, id));
}

unique_ptr<AssignedDataset> AssignedDataset::create(const Time& changed, const std::string& name, const std::string& id)
{
    return unique_ptr<AssignedDataset>(new AssignedDataset(changed, name, id));
}

static MetadataType assigneddatasetType = MetadataType::create<AssignedDataset>();

}
}

#include <arki/types.tcc>
