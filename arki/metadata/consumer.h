#ifndef ARKI_METADATA_CONSUMER_H
#define ARKI_METADATA_CONSUMER_H

/*
 * metadata/consumer - Metadata consumer interface and tools
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <string>
#include <iosfwd>
#include <arki/defs.h>

struct lua_State;

namespace arki {
class Metadata;
class Summary;
class Matcher;
class Formatter;

namespace emitter {
class JSON;
}

namespace metadata {

/**
 * Generic interface for metadata observer, used to process a stream of
 * metadata, but leaving their ownership to the caller.
 */
struct Eater
{
    virtual ~Eater() {}
    /**
     * Observer a metadata, without changing it.
     *
     * If the result is true, then the observer is happy to continue observing
     * more metadata. If it's false, then the observer is satisfied and must
     * not be sent any more metadata.
     */
    virtual bool eat(std::auto_ptr<Metadata> md) = 0;

    /// Push to the LUA stack a userdata to access this Consumer
    void lua_push(lua_State* L);
};

/**
 * Generic interface for metadata observer, used to process a stream of
 * metadata, but leaving their ownership to the caller.
 */
struct Observer
{
    virtual ~Observer() {}
    /**
     * Observer a metadata, without changing it.
     *
     * If the result is true, then the observer is happy to continue observing
     * more metadata. If it's false, then the observer is satisfied and must
     * not be sent any more metadata.
     */
    virtual bool observe(const Metadata&) = 0;

    /// Push to the LUA stack a userdata to access this Consumer
    void lua_push(lua_State* L);
};

/// Pass through metadata to a consumer if it matches a Matcher expression
struct FilteredEater : public metadata::Eater
{
    const Matcher& matcher;
    metadata::Eater& next;
    FilteredEater(const Matcher& matcher, metadata::Eater& next)
        : matcher(matcher), next(next) {}

    bool eat(std::auto_ptr<Metadata> md) override;
};

/// Pass through metadata to a consumer if it matches a Matcher expression
struct FilteredObserver : public metadata::Observer
{
    const Matcher& matcher;
    metadata::Observer& next;
    FilteredObserver(const Matcher& matcher, metadata::Observer& next)
        : matcher(matcher), next(next) {}
    bool observe(const Metadata& md) override;
};


// Metadata consumer that passes the metadata to a Lua function
struct LuaConsumer : public Eater
{
	lua_State* L;
	int funcid;

	LuaConsumer(lua_State* L, int funcid);
	virtual ~LuaConsumer();
    bool eat(std::auto_ptr<Metadata> md) override;

	static std::auto_ptr<LuaConsumer> lua_check(lua_State* L, int idx);
};

struct SummarisingObserver : public Observer
{
    Summary& s;
    SummarisingObserver(Summary& s) : s(s) {}

    bool observe(const Metadata& md) override;
};

struct SummarisingEater : public Eater
{
    Summary& s;
    SummarisingEater(Summary& s) : s(s) {}

    bool eat(std::auto_ptr<Metadata> md) override;
};

struct Counter : public Observer, Eater
{
	size_t count;

	Counter() : count(0) {}

    bool eat(std::auto_ptr<Metadata> md) override { ++count; return true; }
    bool observe(const Metadata& md) override { ++count; return true; }
};

// Generic simple hook interface
struct Hook
{
    virtual ~Hook() {}

    virtual void operator()() = 0;
};


}
}

// vim:set ts=4 sw=4:
#endif
