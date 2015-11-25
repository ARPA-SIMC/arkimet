#include <arki/summary/stats.h>
#include <arki/metadata.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/utils/lua.h>
#include <arki/utils/string.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace arki::types;

namespace arki {

namespace types {
const char* traits<summary::Stats>::type_tag = "summarystats";
const types::Code traits<summary::Stats>::type_code = types::TYPE_SUMMARYSTATS;
const size_t traits<summary::Stats>::type_sersize_bytes = 2;
const char* traits<summary::Stats>::type_lua_tag = LUATAG_TYPES ".summary.stats";
}

namespace summary {

Stats::Stats()
    : count(0), size(0), begin(0, 0, 0), end(0, 0, 0)
{
}

Stats::Stats(const Metadata& md)
    : count(1), size(md.data_size()), begin(0, 0, 0), end(0, 0, 0)
{
    if (const Reftime* rt = md.get<types::Reftime>())
    {
        begin = rt->period_begin();
        end = rt->period_end();
    } else
        throw wibble::exception::Consistency("summarising metadata", "missing reference time");
}

Stats* Stats::clone() const
{
    return new Stats(*this);
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

    if (int res = count - v->count) return res;
    if (int res = size - v->size) return res;
    if (int res = begin.compare(v->begin)) return res;
    return end.compare(v->end);
}

bool Stats::equals(const Type& o) const
{
    const Stats* v = dynamic_cast<const Stats*>(&o);
    if (!v) return false;
    return count == v->count && size == v->size && begin == v->begin && end == v->end;
}

void Stats::merge(const Stats& s)
{
    if (s.count == 0)
        return;

    if (count == 0)
    {
        begin = s.begin;
        end = s.end;
    } else {
        if (begin > s.begin) begin = s.begin;
        if (end < s.end) end = s.end;
    }
    count += s.count;
    size += s.size;
}

void Stats::merge(const Metadata& md)
{
    if (const Reftime* rt = md.get<types::Reftime>())
    {
        if (count == 0)
        {
            begin = rt->period_begin();
            end = rt->period_end();
        } else
            rt->expand_date_range(begin, end);
    }
    else
        throw wibble::exception::Consistency("summarising metadata", "missing reference time");
    ++count;
    size += md.data_size();
}

std::unique_ptr<types::Reftime> Stats::make_reftime() const
{
    return Reftime::create(begin, end);
}

void Stats::encodeWithoutEnvelope(Encoder& enc) const
{
    unique_ptr<types::Reftime> reftime(Reftime::create(begin, end));
    enc.addUInt(count, 4);
    enc.addString(reftime->encodeBinary());
    enc.addULInt(size, 8);
}

std::ostream& Stats::writeToOstream(std::ostream& o) const
{
    return o << toYaml(0);
}

void Stats::serialiseLocal(Emitter& e, const Formatter* f) const
{
    if (count > 0)
    {
        e.add("b"); begin.serialiseList(e);
        e.add("e"); end.serialiseList(e);
    }
    e.add("c", count);
    e.add("s", size);
}

unique_ptr<Stats> Stats::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    unique_ptr<Stats> res(new Stats);
    res->count = val["c"].want_int("parsing summary stats count");
    res->size = val["s"].want_int("parsing summary stats size");
    if (res->count)
    {
        res->begin = *types::Time::decodeList(val["b"].want_list("parsing summary stats begin"));
        res->end = *types::Time::decodeList(val["e"].want_list("parsing summary stats end"));
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
    unique_ptr<types::Reftime> reftime(Reftime::create(begin, end));
    string ind(indent, ' ');
    out << ind << "Count: " << count << endl;
    out << ind << "Size: " << size << endl;
    out << ind << "Reftime: " << *reftime << endl;
}

unique_ptr<Stats> Stats::decode(const unsigned char* buf, size_t len)
{
    using namespace utils::codec;

    unique_ptr<Stats> res(new Stats);

    // First decode the count
    if (len < 4)
    {
        stringstream ss;
        ss << "cannot parse summary stats: size is " << len << " but at least 4 bytes are needed";
        throw std::runtime_error(ss.str());
    }
    res->count = decodeUInt(buf, 4);
    buf += 4; len -= 4;

    // Then decode the reftime
    const unsigned char* el_start = buf;
    size_t el_len = len;
    types::Code el_type = types::decodeEnvelope(el_start, el_len);
    if (el_type == types::TYPE_REFTIME)
    {
        unique_ptr<Reftime> rt(Reftime::decode(el_start, el_len));
        res->begin = rt->period_begin();
        res->end = rt->period_end();
    }
    else
    {
        stringstream ss;
        ss << "cannot parse summary stats: cannot handle element " << el_type;
        throw std::runtime_error(ss.str());
    }
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

    return res;
}

unique_ptr<Stats> Stats::decodeString(const std::string& str)
{
    unique_ptr<Stats> res(new Stats);
    stringstream in(str, ios_base::in);
    wibble::str::YamlStream yamlStream;
    for (wibble::str::YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        string name = str::lower(i->first);
        if (name == "count")
            res->count = strtoul(i->second.c_str(), 0, 10);
        else if (name == "size")
            res->size = strtoull(i->second.c_str(), 0, 10);
        else if (name == "reftime")
        {
            unique_ptr<Reftime> rt(Reftime::decodeString(i->second));
            res->begin = rt->period_begin();
            res->end = rt->period_end();
        }
    }
    return res;
}

#ifdef HAVE_LUA
bool Stats::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "count")
        lua_pushnumber(L, count);
    else if (name == "reftime")
        Reftime::create(begin, end)->lua_push(L);
    else
        return types::CoreType<Stats>::lua_lookup(L, name);
    return true;
}
#endif

static types::MetadataType summaryStatsType = types::MetadataType::create<summary::Stats>();

}
}
#include <arki/types.tcc>
