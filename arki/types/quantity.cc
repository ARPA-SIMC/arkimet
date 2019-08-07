#include "arki/exceptions.h"
#include "arki/types/quantity.h"
#include "arki/types/utils.h"
#include "arki/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif

#define CODE TYPE_QUANTITY
#define TAG "quantity"
#define SERSIZELEN 	1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Quantity>::type_tag = TAG;
const types::Code traits<Quantity>::type_code = CODE;
const size_t traits<Quantity>::type_sersize_bytes = SERSIZELEN;
const char* traits<Quantity>::type_lua_tag = LUATAG_TYPES ".quantity";

int Quantity::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Quantity* v = dynamic_cast<const Quantity*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

    std::ostringstream ss1;
    std::ostringstream ss2;

    writeToOstream(ss1);
    v->writeToOstream(ss2);

    return ss1.str().compare(ss2.str());
}

bool Quantity::equals(const Type& o) const
{
	const Quantity* v = dynamic_cast<const Quantity*>(&o);
	if (!v) return false;

	return compare(*v) == 0;
}

void Quantity::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    enc.add_varint(values.size());

    for (const auto& v: values)
    {
        enc.add_varint(v.size());
        enc.add_raw(v);
    }
}

unique_ptr<Quantity> Quantity::decode(BinaryDecoder& dec)
{
    size_t num = dec.pop_varint<size_t>("quantity num elemetns");
    std::set<std::string> vals;

    for (size_t i=0; i<num; i++)
    {
        size_t vallen = dec.pop_varint<size_t>("quantity name len");
        string val = dec.pop_string(vallen, "quantity name");
        vals.insert(val);
    }

    return Quantity::create(vals);
}

std::ostream& Quantity::writeToOstream(std::ostream& o) const
{
    return o << str::join(", ", values.begin(), values.end());
}

void Quantity::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.quantity_value);
    e.start_list();
    for (const auto& value: values)
        e.add(value);
    e.end_list();
}

std::unique_ptr<Quantity> Quantity::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    std::set<string> vals;
    val.sub(keys.quantity_value, "Quantity values", [&](const structured::Reader& list) {
        unsigned size = list.list_size("Quantity values");
        for (unsigned i = 0; i < size; ++i)
            vals.insert(list.as_string(i, "quantity value"));
    });
    return Quantity::create(vals);
}

unique_ptr<Quantity> Quantity::decodeString(const std::string& val)
{
	if (val.empty())
		throw_consistency_error("parsing Quantity", "string is empty");

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
	static const struct luaL_Reg lib [] = {
		{ "new", arkilua_new_quantity },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_quantity", lib);
}
#endif

Quantity* Quantity::clone() const
{
    return new Quantity(values);
}

unique_ptr<Quantity> Quantity::create(const std::string& values)
{
    std::set<std::string> vals;
    split(values, vals);
    return Quantity::create(vals);
}

unique_ptr<Quantity> Quantity::create(const std::set<std::string>& values)
{
    return unique_ptr<Quantity>(new Quantity(values));
}


void Quantity::init()
{
    MetadataType::register_type<Quantity>();
}

}
}
#include <arki/types/core.tcc>
