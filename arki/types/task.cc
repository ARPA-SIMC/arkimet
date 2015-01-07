/*
 * types/task - Metadata task
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/task.h>
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

#define CODE 		types::TYPE_TASK
#define TAG 		"task"
#define SERSIZELEN 	1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki { namespace types {

/*============================================================================*/

const char* 		traits<Task>::type_tag 			= TAG;
const types::Code 	traits<Task>::type_code 		= CODE;
const size_t 		traits<Task>::type_sersize_bytes 	= SERSIZELEN;
const char* 		traits<Task>::type_lua_tag 		= LUATAG_TYPES ".task";

/*============================================================================*/

int Task::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Task* v = dynamic_cast<const Task*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
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

void Task::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addVarint(task.size());
	enc.addString(task);
}

auto_ptr<Task> Task::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;

	ensureSize(len, 1, "task");
	Decoder dec(buf, len);
	size_t vallen 	= dec.popVarint<size_t>("task text size");
	string val 	= dec.popString(vallen, "task text");
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

auto_ptr<Task> Task::decodeMapping(const emitter::memory::Mapping& val)
{
    return Task::create(val["va"].want_string("parsing Task value"));
}

auto_ptr<Task> Task::decodeString(const std::string& val)
{
	if (val.empty())
		throw wibble::exception::Consistency("parsing Task", "string is empty");
	//if (val[0] != '[')
	//	throw wibble::exception::Consistency("parsing Task", "string does not start with open square bracket");
	//size_t pos = val.find(']');
	//if (pos == string::npos)
	//	throw wibble::exception::Consistency("parsing Task", "no closed square bracket found");
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

auto_ptr<Task> Task::create(const std::string& val)
{
    return auto_ptr<Task>(new Task(val));
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
