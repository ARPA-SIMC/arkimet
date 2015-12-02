#include <arki/wibble/exception.h>
#include <arki/types/reftime.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/utils/string.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <cmath>
#include <cstring>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_REFTIME
#define TAG "reftime"
#define SERSIZELEN 1

using namespace std;
using namespace wibble;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {
	
const char* traits<Reftime>::type_tag = TAG;
const types::Code traits<Reftime>::type_code = CODE;
const size_t traits<Reftime>::type_sersize_bytes = SERSIZELEN;
const char* traits<Reftime>::type_lua_tag = LUATAG_TYPES ".reftime";

// Style constants
const unsigned char Reftime::POSITION;
const unsigned char Reftime::PERIOD;

Reftime::Style Reftime::parseStyle(const std::string& str)
{
	if (str == "POSITION") return POSITION;
	if (str == "PERIOD") return PERIOD;
	throw wibble::exception::Consistency("parsing Reftime style", "cannot parse Reftime style '"+str+"': only POSITION and PERIOD are supported");
}

std::string Reftime::formatStyle(Reftime::Style s)
{
	switch (s)
	{
		case Reftime::POSITION: return "POSITION";
		case Reftime::PERIOD: return "PERIOD";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<Reftime> Reftime::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Reftime");
	Style s = (Style)decodeUInt(buf, 1);
    switch (s)
    {
        case POSITION:
            ensureSize(len, 6, "Reftime");
            return Reftime::createPosition(*Time::decode(buf+1, 5));
        case PERIOD:
            ensureSize(len, 11, "Reftime");
            return Reftime::createPeriod(*Time::decode(buf+1, 5), *Time::decode(buf+6, 5));
        default:
        {
            stringstream ss;
            ss << "cannot parse reference time: style is " << s << " but we can only decode POSITION and PERIOD";
            throw std::runtime_error(ss.str());
        }
    }
}

unique_ptr<Reftime> Reftime::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case POSITION: return upcast<Reftime>(reftime::Position::decodeMapping(val));
        case PERIOD: return upcast<Reftime>(reftime::Period::decodeMapping(val));
        default:
            throw wibble::exception::Consistency("parsing Reftime", "unknown Reftime style " + val.get_string());
    }
}

unique_ptr<Reftime> Reftime::decodeString(const std::string& val)
{
    size_t pos = val.find(" to ");
    if (pos == string::npos) return Reftime::createPosition(*Time::decodeString(val));

    return Reftime::createPeriod(
                *Time::decodeString(val.substr(0, pos)),
                *Time::decodeString(val.substr(pos + 4)));
}

static int arkilua_new_position(lua_State* L)
{
    const Time* time = types::Type::lua_check<Time>(L, 1);
    reftime::Position::create(*time)->lua_push(L);
    return 1;
}

static int arkilua_new_period(lua_State* L)
{
    const Time* beg = types::Type::lua_check<Time>(L, 1);
    const Time* end = types::Type::lua_check<Time>(L, 2);
    reftime::Period::create(*beg, *end)->lua_push(L);
    return 1;
}

void Reftime::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "position", arkilua_new_position },
		{ "period", arkilua_new_period },
		{ NULL, NULL }
	};

    utils::lua::add_global_library(L, "arki_reftime", lib);
}

std::unique_ptr<Reftime> Reftime::create(const Time& begin, const Time& end)
{
    if (begin == end)
        return upcast<Reftime>(reftime::Position::create(begin));
    else
        return upcast<Reftime>(reftime::Period::create(begin, end));
}

std::unique_ptr<Reftime> Reftime::createPosition(const Time& position)
{
    return upcast<Reftime>(reftime::Position::create(position));
}

std::unique_ptr<Reftime> Reftime::createPeriod(const Time& begin, const Time& end)
{
    return upcast<Reftime>(reftime::Period::create(begin, end));
}

namespace reftime {

Position::Position(const Time& time) : time(time) {}

Reftime::Style Position::style() const { return Reftime::POSITION; }

void Position::encodeWithoutEnvelope(Encoder& enc) const
{
    Reftime::encodeWithoutEnvelope(enc);
    time.encodeWithoutEnvelope(enc);
}

std::ostream& Position::writeToOstream(std::ostream& o) const
{
    return time.writeToOstream(o);
}

void Position::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Reftime::serialiseLocal(e, f);
    e.add("ti");
    time.serialiseList(e);
}

unique_ptr<Position> Position::decodeMapping(const emitter::memory::Mapping& val)
{
    unique_ptr<Time> time = Time::decodeList(val["ti"].want_list("parsing position reftime time"));
    return Position::create(*time);
}

std::string Position::exactQuery() const
{
	return "=" + time.toISO8601();
}

const char* Position::lua_type_name() const { return "arki.types.reftime.position"; }

bool Position::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "time")
        time.lua_push(L);
    else
        return Reftime::lua_lookup(L, name);
    return true;
}

int Position::compare_local(const Reftime& o) const
{
	// We should be the same kind, so upcast
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Position Reftime, but is a ") + typeid(&o).name() + " instead");

	return time.compare(v->time);
}

bool Position::equals(const Type& o) const
{
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v) return false;
	return time == v->time;
}

void Position::expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const
{
    if (!begin.get() || *begin > time)
        begin.reset(new Time(time));

    if (!end.get() || *end < time)
        end.reset(new Time(time));
}

void Position::expand_date_range(types::Time& begin, types::Time& end) const
{
    if (begin > time) begin = time;
    if (end < time) end = time;
}

Position* Position::clone() const
{
    return new Position(time);
}

unique_ptr<Position> Position::create(const Time& time)
{
    return unique_ptr<Position>(new Position(time));
}

Period::Period(const Time& begin, const Time& end)
    : begin(begin), end(end) {}

Reftime::Style Period::style() const { return Reftime::PERIOD; }

void Period::encodeWithoutEnvelope(Encoder& enc) const
{
    Reftime::encodeWithoutEnvelope(enc);
    begin.encodeWithoutEnvelope(enc);
    end.encodeWithoutEnvelope(enc);
}

std::ostream& Period::writeToOstream(std::ostream& o) const
{
    begin.writeToOstream(o);
    o << " to ";
    return end.writeToOstream(o);
}

void Period::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Reftime::serialiseLocal(e, f);
    e.add("b"); begin.serialiseList(e);
    e.add("e"); end.serialiseList(e);
}

unique_ptr<Period> Period::decodeMapping(const emitter::memory::Mapping& val)
{
    unique_ptr<Time> beg = Time::decodeList(val["b"].want_list("parsing period reftime begin"));
    unique_ptr<Time> end = Time::decodeList(val["e"].want_list("parsing period reftime end"));
    return Period::create(*beg, *end);
}

const char* Period::lua_type_name() const { return "arki.types.reftime.period"; }

bool Period::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "from")
        begin.lua_push(L);
    else if (name == "to")
        end.lua_push(L);
    else
        return Reftime::lua_lookup(L, name);
    return true;
}

int Period::compare_local(const Reftime& o) const
{
	// We should be the same kind, so upcast
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Period Reftime, but is a ") + typeid(&o).name() + " instead");

    if (int res = begin.compare(v->begin)) return res;
    return end.compare(v->end);
}

bool Period::equals(const Type& o) const
{
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v) return false;
	return begin == v->begin && end == v->end;
}

void Period::expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const
{
    if (!begin.get() || *begin > this->begin)
        begin.reset(new Time(this->begin));

    if (!end.get() || *end < this->end)
        end.reset(new Time(this->end));
}

void Period::expand_date_range(types::Time& begin, types::Time& end) const
{
    if (begin > this->begin) begin = this->begin;
    if (end < this->end) end = this->end;
}

Period* Period::clone() const
{
    return new Period(begin, end);
}

unique_ptr<Period> Period::create(const Time& begin, const Time& end)
{
    return unique_ptr<Period>(new Period(begin, end));
}

}

void Reftime::init()
{
    MetadataType::register_type<Reftime>();
}

}
}

#include <arki/types.tcc>

// vim:set ts=4 sw=4:
