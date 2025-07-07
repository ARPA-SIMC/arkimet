#ifndef ARKI_SUMMARY_STATS_H
#define ARKI_SUMMARY_STATS_H

/// summary/stats - Implementation of a summary stats payload

#include <arki/core/time.h>
#include <arki/metadata/fwd.h>
#include <arki/structured/fwd.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

namespace arki {
namespace summary {

class Stats
{
public:
    size_t count;
    uint64_t size;
    core::Time begin;
    core::Time end;

    Stats();
    Stats(const Metadata& md);

    bool operator==(const Stats& o) const { return compare(o) == 0; }
    bool operator!=(const Stats& o) const { return compare(o) != 0; }

    int compare(const Stats& o) const;
    bool equals(const Stats& o) const;

    void merge(const Stats& s);
    void merge(const Metadata& md);

    core::Interval make_interval() const;

    void encodeBinary(core::BinaryEncoder& enc) const;

    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const;
    std::ostream& writeToOstream(std::ostream& o) const;
    void serialiseLocal(structured::Emitter& e, const Formatter* f = 0) const;
    std::string toYaml(size_t indent = 0) const;
    void toYaml(std::ostream& out, size_t indent = 0) const;
    static std::unique_ptr<Stats> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Stats> decodeString(const std::string& str);
    static std::unique_ptr<Stats>
    decode_structure(const structured::Keys& keys,
                     const structured::Reader& val);

    Stats* clone() const;
};

} // namespace summary
} // namespace arki
#endif
