/*
 * summary/stats - Implementation of a summary stats payload
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

#include <arki/summary/stats.h>
#include <arki/metadata.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils::codec;

namespace arki {

namespace types {
const char* traits<summary::Stats>::type_tag = "summarystats";
const types::Code traits<summary::Stats>::type_code = types::TYPE_SUMMARYSTATS;
const size_t traits<summary::Stats>::type_sersize_bytes = 2;
const char* traits<summary::Stats>::type_lua_tag = LUATAG_TYPES ".summary.stats";
}

namespace summary {

Stats::Stats(const Metadata& md)
    : count(1), size(md.dataSize())
{
    reftimeMerger.merge(md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>());
}

Stats::Stats(size_t count, unsigned long long size, const types::Reftime* reftime)
    : count(count), size(size)
{
    reftimeMerger.merge(reftime);
}

int Stats::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Stats* v = dynamic_cast<const Stats*>(&o);
    if (!v)
        throw wibble::exception::Consistency(
            "comparing metadata types",
            string("second element claims to be a summary::Stats, but it is a ") + typeid(&o).name() + " instead");

    return compare(*v);
}

int Stats::compare(const Stats& o) const
{
    if (int res = count - o.count) return res;
    if (int res = size - o.size) return res;
    return reftimeMerger.compare(o.reftimeMerger);
}

bool Stats::operator==(const Type& o) const
{
    const Stats* v = dynamic_cast<const Stats*>(&o);
    if (!v) return false;
    return operator==(*v);
}

bool Stats::operator==(const Stats& o) const
{
    return count == o.count && size == o.size && reftimeMerger == o.reftimeMerger;
}

void Stats::merge(const Stats& s)
{
    count += s.count;
    size += s.size;
    reftimeMerger.merge(s.reftimeMerger);
}

void Stats::merge(size_t ocount, unsigned long long osize, const types::Reftime* reftime)
{
    count += ocount;
    size += osize;
    reftimeMerger.merge(reftime);
}

void Stats::merge(const Metadata& md)
{
    ++count;
    size += md.dataSize();
    reftimeMerger.merge(md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>());
}

void Stats::encodeWithoutEnvelope(Encoder& enc) const
{
    arki::Item<types::Reftime> reftime(reftimeMerger.makeReftime());
    enc.addUInt(count, 4);
    enc.addString(reftime.encode());
    enc.addULInt(size, 8);
}

std::ostream& Stats::writeToOstream(std::ostream& o) const
{
    return o << toYaml(0);
}

void Stats::serialiseLocal(Emitter& e, const Formatter* f) const
{
    if (reftimeMerger.begin)
    {
        if (reftimeMerger.end)
        {
            // Period: begin--end
            e.add("b"); reftimeMerger.begin->serialiseList(e);
            e.add("e"); reftimeMerger.end->serialiseList(e);
        } else {
            // Instant: begin--begin
            e.add("b"); reftimeMerger.begin->serialiseList(e);
            e.add("e"); reftimeMerger.begin->serialiseList(e);
        }
    }
    e.add("c", (long long int)count);
    e.add("s", (long long int)size);
}

UItem<Stats> Stats::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    auto_ptr<Stats> res(new Stats);
    res->count = val["c"].want_int("parsing summary stats count");
    res->size = val["s"].want_int("parsing summary stats size");
    if (!val["b"].is_null())
    {
        Item<types::Time> begin = types::Time::decodeList(val["b"].want_list("parsing summary stats begin"));
        Item<types::Time> end = types::Time::decodeList(val["e"].want_list("parsing summary stats end"));
        if (begin == end)
            res->reftimeMerger.mergeTime(begin);
        else
            res->reftimeMerger.mergeTime(begin, end);
    }
    return res.release();
}

std::string Stats::toYaml(size_t indent) const
{
    stringstream res;
    toYaml(res, indent);
    return res.str();
}

void Stats::toYaml(std::ostream& out, size_t indent) const
{
    arki::Item<types::Reftime> reftime(reftimeMerger.makeReftime());
    string ind(indent, ' ');
    out << ind << "Count: " << count << endl;
    out << ind << "Size: " << size << endl;
    out << ind << "Reftime: " << reftime << endl;
}

UItem<Stats> Stats::decode(const unsigned char* buf, size_t len)
{
    using namespace utils::codec;

    auto_ptr<Stats> res(new Stats);

    // First decode the count
    if (len < 4)
        throw wibble::exception::Consistency("parsing summary stats", "summary stats has size " + str::fmt(len) + " but at least 4 bytes are needed");
    res->count = decodeUInt(buf, 4);
    buf += 4; len -= 4;

    // Then decode the reftime
    const unsigned char* el_start = buf;
    size_t el_len = len;
    types::Code el_type = types::decodeEnvelope(el_start, el_len);
    if (el_type == types::TYPE_REFTIME)
        res->reftimeMerger.merge(types::Reftime::decode(el_start, el_len));
    else
        throw wibble::exception::Consistency("parsing summary stats", "cannot handle element " + str::fmt(el_type));
    len -= el_start + el_len - buf;
    buf = el_start + el_len;

    // Then decode the size (optional, for backward compatibility)
    if (len < 8)
        res->size = 0;
    else
    {
        res->size = decodeULInt(buf, 8);
        buf += 8; len -= 8;
    }

    return res.release();
}

UItem<Stats> Stats::decodeString(const std::string& str)
{
    using namespace str;

    auto_ptr<Stats> res(new Stats);
    stringstream in(str, ios_base::in);
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        string name = tolower(i->first);
        if (name == "count")
            res->count = strtoul(i->second.c_str(), 0, 10);
        else if (name == "size")
            res->size = strtoull(i->second.c_str(), 0, 10);
        else if (name == "reftime")
            res->reftimeMerger.merge(types::Reftime::decodeString(i->second));
    }
    return res.release();
}

#ifdef HAVE_LUA
int Stats::lua_lookup(lua_State* L)
{
    int udataidx = lua_upvalueindex(1);
    int keyidx = lua_upvalueindex(2);
    // Fetch the Stats reference from the userdata value
    luaL_checkudata(L, udataidx, "arki.summarystats");
    void* userdata = lua_touserdata(L, udataidx);
    const Stats& v = **(const Stats**)userdata;

    // Get the name to lookup from lua
    // (we use 2 because 1 is the table, since we are a __index function)
    luaL_checkstring(L, keyidx);
    string name = lua_tostring(L, keyidx);

    if (name == "count")
    {
        lua_pushnumber(L, v.count);
        return 1;
    }
    else if (name == "reftime")
    {
        arki::Item<types::Reftime> reftime(v.reftimeMerger.makeReftime());
        reftime->lua_push(L);
        return 1;
    }
    else
    {
        lua_pushnil(L);
        return 1;
    }
}
static int arkilua_lookup_summarystats(lua_State* L)
{
    // build a closure with the parameters passed, and return it
    lua_pushcclosure(L, Stats::lua_lookup, 2);
    return 1;
}
void Stats::lua_push(lua_State* L) const
{
    // The 'grib' object is a userdata that holds a pointer to this Grib structure
    const Stats** s = (const Stats**)lua_newuserdata(L, sizeof(const Stats*));
    *s = this;

    // Set the metatable for the userdata
    if (luaL_newmetatable(L, "arki.summarystats"));
    {
        // If the metatable wasn't previously created, create it now
        // Set the __index metamethod to the lookup function
        lua_pushstring(L, "__index");
        lua_pushcfunction(L, arkilua_lookup_summarystats);
        lua_settable(L, -3);
    }

    lua_setmetatable(L, -2);
}
#endif

static types::MetadataType summaryStatsType = types::MetadataType::create<summary::Stats>();

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
