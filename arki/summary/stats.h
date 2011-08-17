#ifndef ARKI_SUMMARY_STATS_H
#define ARKI_SUMMARY_STATS_H

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


#include <arki/types.h>
#include <arki/types/reftime.h>
#include <map>
#include <string>
#include <memory>
#include <iosfwd>
#include <stdint.h>

struct lua_State;

namespace arki {
class Metadata;
class Formatter;

namespace summary {
struct Stats;
}

namespace types {
class Reftime;

template<>
struct traits<summary::Stats>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    static const char* type_lua_tag;

    typedef unsigned char Style;
};

}

namespace summary {

struct Stats : public types::CoreType<Stats>
{
    size_t count;
    uint64_t size;
    types::reftime::Collector reftimeMerger;

    Stats() : count(0), size(0) {}
    Stats(const Metadata& md);
    Stats(size_t count, unsigned long long size, const types::Reftime* reftime);

    virtual int compare(const Type& o) const;
    virtual int compare(const Stats& o) const;
    virtual bool operator==(const Type& o) const;
    virtual bool operator==(const Stats& o) const;

    bool operator!=(const Stats& i) const { return !operator==(i); }
    bool operator<(const Stats& i) const { return compare(i) < 0; }

    void merge(const Stats& s);
    void merge(const Metadata& md);
    void merge(size_t count, unsigned long long size, const types::Reftime* reftime);

    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
    std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
    std::string toYaml(size_t indent = 0) const;
    void toYaml(std::ostream& out, size_t indent = 0) const;
    static UItem<Stats> decode(const unsigned char* buf, size_t len);
    static UItem<Stats> decodeString(const std::string& str);
    static UItem<Stats> decodeMapping(const emitter::memory::Mapping& val);

    virtual void lua_push(lua_State* L) const;
    static int lua_lookup(lua_State* L);
};

}
}

// vim:set ts=4 sw=4:
#endif
