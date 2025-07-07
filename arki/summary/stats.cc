#include "arki/summary/stats.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/structured/emitter.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
#include "arki/types/reftime.h"
#include "arki/utils/string.h"
#include "arki/utils/yaml.h"

using namespace std;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

/*
namespace types {
const char* traits<summary::Stats>::type_tag = "summarystats";
const types::Code traits<summary::Stats>::type_code = TYPE_SUMMARYSTATS;
const size_t traits<summary::Stats>::type_sersize_bytes = 2;
}
*/

namespace summary {

Stats::Stats() : count(0), size(0), begin(0, 0, 0), end(0, 0, 0) {}

Stats::Stats(const Metadata& md)
    : count(1), size(md.data_size()), begin(0, 0, 0), end(0, 0, 0)
{
    if (const Reftime* rt = md.get<types::Reftime>())
        begin = end = rt->get_Position();
    else
        throw_consistency_error("summarising metadata",
                                "missing reference time");
}

Stats* Stats::clone() const { return new Stats(*this); }

int Stats::compare(const Stats& o) const
{
    if (int res = count - o.count)
        return res;
    if (int res = size - o.size)
        return res;
    if (int res = begin.compare(o.begin))
        return res;
    return end.compare(o.end);
}

bool Stats::equals(const Stats& o) const
{
    return count == o.count && size == o.size && begin == o.begin &&
           end == o.end;
}

void Stats::merge(const Stats& s)
{
    if (s.count == 0)
        return;

    if (count == 0)
    {
        begin = s.begin;
        end   = s.end;
    }
    else
    {
        if (begin > s.begin)
            begin = s.begin;
        if (end < s.end)
            end = s.end;
    }
    count += s.count;
    size += s.size;
}

void Stats::merge(const Metadata& md)
{
    if (const Reftime* rt = md.get<types::Reftime>())
    {
        auto time = rt->get_Position();
        if (count == 0)
            begin = end = time;
        else
        {
            if (time < begin)
                begin = time;
            if (end < time)
                end = time;
        }
    }
    else
        throw_consistency_error("summarising metadata",
                                "missing reference time");
    ++count;
    size += md.data_size();
}

core::Interval Stats::make_interval() const
{
    // Stats has extremes inclusive
    return core::Interval(begin, end.next_instant());
}

void Stats::encodeBinary(core::BinaryEncoder& enc) const
{
    vector<uint8_t> contents;
    contents.reserve(256);
    core::BinaryEncoder contentsenc(contents);
    encodeWithoutEnvelope(contentsenc);

    enc.add_varint((unsigned)TYPE_SUMMARYSTATS);
    enc.add_varint(contents.size());
    enc.add_raw(contents);
}

void Stats::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_unsigned(count, 4);
    /*
     * This is the serialization of a Reftime type with the obsolete PERIOD
     * style, inlined so we can get rid of reftime::Period.
     */
    enc.add_varint(static_cast<unsigned>(arki::TYPE_REFTIME));
    enc.add_varint(11u);
    enc.add_unsigned(static_cast<uint8_t>(reftime::Style::PERIOD), 1);
    begin.encodeWithoutEnvelope(enc);
    end.encodeWithoutEnvelope(enc);
    enc.add_unsigned(size, 8);
}

std::ostream& Stats::writeToOstream(std::ostream& o) const
{
    return o << toYaml(0);
}

void Stats::serialiseLocal(structured::Emitter& e, const Formatter* f) const
{
    if (count > 0)
    {
        e.add("b");
        e.add(begin);
        e.add("e");
        e.add(end);
    }
    e.add("c", count);
    e.add("s", size);
}

unique_ptr<Stats> Stats::decode_structure(const structured::Keys& keys,
                                          const structured::Reader& val)
{
    using namespace structured::memory;
    unique_ptr<Stats> res(new Stats);
    res->count = val.as_int(keys.summarystats_count, "summary stats count");
    res->size  = val.as_int(keys.summarystats_size, "summary stats size");
    if (res->count)
    {
        res->begin =
            val.as_time(keys.summarystats_begin, "summary stats begin");
        res->end = val.as_time(keys.summarystats_end, "summary stats end");
    }
    return res;
}

std::string Stats::toYaml(size_t indent) const
{
    stringstream res;
    toYaml(res, indent);
    return res.str();
}

void Stats::toYaml(std::ostream& out, size_t indent) const
{
    string ind(indent, ' ');
    out << ind << "Count: " << count << endl;
    out << ind << "Size: " << size << endl;
    out << ind << "Reftime: " << begin << " to " << end << endl;
}

unique_ptr<Stats> Stats::decode(core::BinaryDecoder& dec)
{
    unique_ptr<Stats> res(new Stats);

    // First decode the count
    res->count = dec.pop_uint(4, "summary stats (count)");

    // Then decode the reftime
    TypeCode code;
    core::BinaryDecoder inner = dec.pop_type_envelope(code);
    if (code == TYPE_REFTIME)
    {
        reftime::Style sty =
            static_cast<reftime::Style>(inner.pop_byte("Reftime style"));
        switch (sty)
        {
            case reftime::Style::POSITION:
                res->begin = res->end = Time::decode(inner);
                break;
            case reftime::Style::PERIOD:
                res->begin = Time::decode(inner);
                res->end   = Time::decode(inner);
                break;
            default:
                throw std::runtime_error(
                    "cannot parse Reftime: found unsupported style " +
                    Reftime::formatStyle(sty));
        }
    }
    else
    {
        std::stringstream ss;
        ss << "cannot parse summary stats: cannot handle element "
           << formatCode(code);
        throw std::runtime_error(ss.str());
    }

    // Then decode the size (optional, for backward compatibility)
    if (dec.size < 8)
        res->size = 0;
    else
        res->size = dec.pop_ulint(8, "summary stats (size)");

    return res;
}

unique_ptr<Stats> Stats::decodeString(const std::string& str)
{
    unique_ptr<Stats> res(new Stats);
    auto reader = LineReader::from_chars(str.data(), str.size());
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(*reader);
         i != yamlStream.end(); ++i)
    {
        string name = str::lower(i->first);
        if (name == "count")
            res->count = strtoul(i->second.c_str(), 0, 10);
        else if (name == "size")
            res->size = strtoull(i->second.c_str(), 0, 10);
        else if (name == "reftime")
        {
            size_t pos = i->second.find(" to ");
            if (pos == std::string::npos)
                res->begin = res->end = Time::decodeString(i->second);
            else
            {
                res->begin = Time::decodeString(i->second.substr(0, pos));
                res->end   = Time::decodeString(i->second.substr(pos + 4));
            }
        }
    }
    return res;
}

} // namespace summary
} // namespace arki
