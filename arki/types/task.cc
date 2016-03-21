#include <arki/exceptions.h>
#include <arki/types/task.h>
#include <arki/types/utils.h>
#include <arki/binary.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE TYPE_TASK
#define TAG "task"
#define SERSIZELEN 	1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Task>::type_tag = TAG;
const types::Code traits<Task>::type_code = CODE;
const size_t traits<Task>::type_sersize_bytes = SERSIZELEN;
const char* traits<Task>::type_lua_tag = LUATAG_TYPES ".task";

int Task::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Task* v = dynamic_cast<const Task*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

    return task.compare(v->task);
}

bool Task::equals(const Type& o) const
{
	const Task* v = dynamic_cast<const Task*>(&o);
	if (!v) return false;
	return task == v->task;
}

void Task::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    enc.add_varint(task.size());
    enc.add_raw(task);
}

unique_ptr<Task> Task::decode(BinaryDecoder& dec)
{
    size_t vallen = dec.pop_varint<size_t>("task text size");
    string val = dec.pop_string(vallen, "task text");
    return Task::create(val);
}

std::ostream& Task::writeToOstream(std::ostream& o) const
{
	return o << task;
}

void Task::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("va", task);
}

unique_ptr<Task> Task::decodeMapping(const emitter::memory::Mapping& val)
{
    return Task::create(val["va"].want_string("parsing Task value"));
}

unique_ptr<Task> Task::decodeString(const std::string& val)
{
	if (val.empty())
		throw_consistency_error("parsing Task", "string is empty");
	//if (val[0] != '[')
	//	throw_consistency_error("parsing Task", "string does not start with open square bracket");
	//size_t pos = val.find(']');
	//if (pos == string::npos)
	//	throw_consistency_error("parsing Task", "no closed square bracket found");
	return Task::create(val);
}

#ifdef HAVE_LUA
bool Task::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "task")
		lua_pushlstring(L, task.data(), task.size());
	else
		return CoreType<Task>::lua_lookup(L, name);
	return true;
}

static int arkilua_new_task(lua_State* L)
{
	const char* value = luaL_checkstring(L, 1);
	Task::create(value)->lua_push(L);
	return 1;
}

void Task::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "new", arkilua_new_task },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_task", lib);
}
#endif

Task* Task::clone() const
{
    return new Task(task);
}

unique_ptr<Task> Task::create(const std::string& val)
{
    return unique_ptr<Task>(new Task(val));
}

/*============================================================================*/

void Task::init()
{
    MetadataType::register_type<Task>();
}

/*============================================================================*/

}
}

#include <arki/types.tcc>
// vim:set ts=4 sw=4:
