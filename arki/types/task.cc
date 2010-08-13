/*
 * types/task - Metadata task
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/task.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
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

	return compare(*v);
}

int Task::compare(const Task& o) const
{
	return task.compare(o.task);
}

bool Task::operator==(const Type& o) const
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

Item<Task> Task::decode(const unsigned char* buf, size_t len)
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

Item<Task> Task::decodeString(const std::string& val)
{
	if (val.empty())
		throw wibble::exception::Consistency("parsing Task", "string is empty");
	//if (val[0] != '[')
	//	throw wibble::exception::Consistency("parsing Task", "string does not start with open square bracket");
	//size_t pos = val.find(']');
	//if (pos == string::npos)
	//	throw wibble::exception::Consistency("parsing Task", "no closed square bracket found");
	//return Note::create(Time::createFromISO8601(val.substr(1, pos-1)), val.substr(pos+1));
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
#endif

Item<Task> Task::create(const std::string& val)
{
	return new Task(val);
}

/*============================================================================*/

static MetadataType taskType = MetadataType::create<Task>();

/*============================================================================*/

} }

#include <arki/types.tcc>
// vim:set ts=4 sw=4:




























