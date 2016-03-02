#ifndef ARKI_SUMMARY_STATS_H
#define ARKI_SUMMARY_STATS_H

/// summary/stats - Implementation of a summary stats payload

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
}

namespace summary {

struct Stats
{
    size_t count;
    uint64_t size;
    types::Time begin;
    types::Time end;

    Stats();
    Stats(const Metadata& md);

    bool operator==(const Stats& o) const { return compare(o) == 0; }
    bool operator!=(const Stats& o) const { return compare(o) != 0; }

    int compare(const Stats& o) const;
    bool equals(const Stats& o) const;

    void merge(const Stats& s);
    void merge(const Metadata& md);

    std::unique_ptr<types::Reftime> make_reftime() const;

    void encodeBinary(BinaryEncoder& enc) const;

    void encodeWithoutEnvelope(BinaryEncoder& enc) const;
    std::ostream& writeToOstream(std::ostream& o) const;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const;
    std::string toYaml(size_t indent = 0) const;
    void toYaml(std::ostream& out, size_t indent = 0) const;
    static std::unique_ptr<Stats> decode(BinaryDecoder& dec);
    static std::unique_ptr<Stats> decodeString(const std::string& str);
    static std::unique_ptr<Stats> decodeMapping(const emitter::memory::Mapping& val);

    Stats* clone() const;

    /// Push to the LUA stack a userdata with a copy of this item
    void lua_push(lua_State* L) const;

    bool lua_lookup(lua_State* L, const std::string& name) const;
};

}
}
#endif
