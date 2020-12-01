#include "reftime.h"
#include "utils.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "config.h"
#include <sstream>
#include <cmath>
#include <cstring>

#define CODE TYPE_REFTIME
#define TAG "reftime"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace types {
	
const char* traits<Reftime>::type_tag = TAG;
const types::Code traits<Reftime>::type_code = CODE;
const size_t traits<Reftime>::type_sersize_bytes = SERSIZELEN;

Reftime::Style Reftime::parseStyle(const std::string& str)
{
	if (str == "POSITION") return reftime::Style::POSITION;
	if (str == "PERIOD") return reftime::Style::PERIOD;
	throw_consistency_error("parsing Reftime style", "cannot parse Reftime style '"+str+"': only POSITION and PERIOD are supported");
}

std::string Reftime::formatStyle(Reftime::Style s)
{
	switch (s)
	{
		case Style::POSITION: return "POSITION";
		case Style::PERIOD: return "PERIOD";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<Reftime> Reftime::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "reftime style");
    switch (s)
    {
        case Style::POSITION: return Reftime::createPosition(Time::decode(dec));
        case Style::PERIOD:
        {
            auto begin = Time::decode(dec);
            auto until = Time::decode(dec);
            return Reftime::createPeriod(begin, until);
        }
        default:
            throw std::runtime_error("cannot parse reference time: style is '" + std::to_string((int)s) + "' but onlt POSITION and PERIOD are valid values");
    }
}

std::unique_ptr<Reftime> Reftime::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::POSITION: return upcast<Reftime>(reftime::Position::decode_structure(keys, val));
        case Style::PERIOD: return upcast<Reftime>(reftime::Period::decode_structure(keys, val));
        default: throw std::runtime_error("unknown Reftime style");
    }
}

unique_ptr<Reftime> Reftime::decodeString(const std::string& val)
{
    size_t pos = val.find(" to ");
    if (pos == string::npos) return Reftime::createPosition(Time::decodeString(val));

    return Reftime::createPeriod(
                Time::decodeString(val.substr(0, pos)),
                Time::decodeString(val.substr(pos + 4)));
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

Reftime::Style Position::style() const { return Style::POSITION; }

void Position::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Reftime::encodeWithoutEnvelope(enc);
    time.encodeWithoutEnvelope(enc);
}

std::ostream& Position::writeToOstream(std::ostream& o) const
{
    return o << time;
}

void Position::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Reftime::serialise_local(e, keys, f);
    e.add(keys.reftime_position_time);
    e.add(time);
}

std::unique_ptr<Position> Position::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Position::create(val.as_time(keys.reftime_position_time, "time"));
}

std::string Position::exactQuery() const
{
    return "=" + time.to_iso8601();
}

int Position::compare_local(const Reftime& o) const
{
    if (int res = Reftime::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v)
		throw_consistency_error(
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

void Position::expand_date_range(core::Interval& interval) const
{
    if (interval.begin.ye == 0 || interval.begin > time)
        interval.begin = time;

    if (interval.end.ye == 0 || interval.end <= time)
    {
        interval.end = time;
        interval.end.se += 1;
        interval.end.normalise();
    }
}

void Position::expand_date_range(Time& begin, Time& end) const
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

Reftime::Style Period::style() const { return Style::PERIOD; }

void Period::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Reftime::encodeWithoutEnvelope(enc);
    begin.encodeWithoutEnvelope(enc);
    end.encodeWithoutEnvelope(enc);
}

std::ostream& Period::writeToOstream(std::ostream& o) const
{
    return o << begin << " to " << end;
}

void Period::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Reftime::serialise_local(e, keys, f);
    e.add(keys.reftime_period_begin); e.add(begin);
    e.add(keys.reftime_period_end); e.add(end);
}

std::unique_ptr<Period> Period::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Period::create(
            val.as_time(keys.reftime_period_begin, "period begin"),
            val.as_time(keys.reftime_period_end, "period end"));
}

int Period::compare_local(const Reftime& o) const
{
    if (int res = Reftime::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v)
		throw_consistency_error(
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

void Period::expand_date_range(core::Interval& interval) const
{
    if (interval.begin.ye == 0 || interval.begin > begin)
        interval.begin = begin;

    if (interval.end.ye == 0 || interval.end <= end)
    {
        interval.end = end;
        interval.end.se += 1;
        interval.end.normalise();
    }
}

void Period::expand_date_range(Time& begin, Time& end) const
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

#include <arki/types/styled.tcc>
