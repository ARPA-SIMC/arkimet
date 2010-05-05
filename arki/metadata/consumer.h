#ifndef ARKI_METADATA_CONSUMER_H
#define ARKI_METADATA_CONSUMER_H

/*
 * metadata/consumer - Metadata consumer interface and tools
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
 */

#include <memory>

struct lua_State;

namespace arki {
class Metadata;
class Summary;
class Matcher;

namespace metadata {

/**
 * Generic interface for metadata consumers, used to handle a stream of
 * metadata, such as after scanning a file, or querying a dataset.
 */
struct Consumer
{
	virtual ~Consumer() {}
	/**
	 * Consume a metadata.
	 *
	 * If the result is true, then the consumer is happy to accept more
	 * metadata.  If it's false, then the consume is satisfied and must not be
	 * sent any more metadata.
	 */
	virtual bool operator()(Metadata&) = 0;

	/// Push to the LUA stack a userdata to access this Consumer
	void lua_push(lua_State* L);
};

// Pass through metadata to a consumer if it matches a Matcher expression
struct FilteredConsumer : public Consumer
{
	const Matcher& matcher;
	metadata::Consumer& next;

	FilteredConsumer(const Matcher& matcher, Consumer& next)
		: matcher(matcher), next(next) {}
	~FilteredConsumer() {}

	bool operator()(Metadata& m);
};

// Metadata consumer that passes the metadata to a Lua function
struct LuaConsumer : public Consumer
{
	lua_State* L;
	int funcid;

	LuaConsumer(lua_State* L, int funcid);
	virtual ~LuaConsumer();
	virtual bool operator()(Metadata&);

	static std::auto_ptr<LuaConsumer> lua_check(lua_State* L, int idx);
};

struct Summarise : public Consumer
{
	Summary& s;
	Summarise(Summary& s) : s(s) {}

	bool operator()(Metadata& md);
};

}
}

// vim:set ts=4 sw=4:
#endif
