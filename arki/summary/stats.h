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
    types::Time begin;
    types::Time end;

    Stats();
    Stats(const Metadata& md);

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    void merge(const Stats& s);
    void merge(const Metadata& md);

    std::unique_ptr<types::Reftime> make_reftime() const;

    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string toYaml(size_t indent = 0) const;
    void toYaml(std::ostream& out, size_t indent = 0) const;
    static std::unique_ptr<Stats> decode(BinaryDecoder& dec);
    static std::unique_ptr<Stats> decodeString(const std::string& str);
    static std::unique_ptr<Stats> decodeMapping(const emitter::memory::Mapping& val);

    Stats* clone() const override;

    bool lua_lookup(lua_State* L, const std::string& name) const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
