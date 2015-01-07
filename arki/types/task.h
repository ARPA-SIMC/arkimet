#ifndef ARKI_TYPES_TASK_H
#define ARKI_TYPES_TASK_H

/*
 * types/task - Metadata task (used for OdimH5 /how.task)
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

#include <arki/types.h>

struct lua_State;

namespace arki { namespace types {

/*============================================================================*/

struct Task;

template<> struct traits<Task>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/*============================================================================*/

/**
 * A Task annotation
 */
struct Task : public CoreType<Task>
{
	std::string task;

	Task(const std::string& value) : task(value) {}

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    static std::auto_ptr<Task> decode(const unsigned char* buf, size_t len);
    static std::auto_ptr<Task> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    Task* clone() const override;

    /// Create a task
    static std::auto_ptr<Task> create(const std::string& value);
    static std::auto_ptr<Task> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

/*============================================================================*/

} }

// vim:set ts=4 sw=4:
#endif



















