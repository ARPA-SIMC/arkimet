#include "arki/exceptions.h"
#include "arki/types/run.h"
#include "arki/types/utils.h"
#include "arki/utils/iostream.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <iomanip>
#include <sstream>

#define CODE TYPE_RUN
#define TAG "run"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Run>::type_tag = TAG;
const types::Code traits<Run>::type_code = CODE;
const size_t traits<Run>::type_sersize_bytes = SERSIZELEN;

Run::Style Run::parseStyle(const std::string& str)
{
    if (str == "MINUTE") return Style::MINUTE;
    throw_consistency_error("parsing Run style", "cannot parse Run style '"+str+"': only MINUTE is supported");
}

std::string Run::formatStyle(Run::Style s)
{
    switch (s)
    {
        case Style::MINUTE: return "MINUTE";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

unique_ptr<Run> Run::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "run style");
    switch (s)
    {
        case Style::MINUTE: {
            unsigned int m = dec.pop_varint<unsigned>("run minute");
            return createMinute(m / 60, m % 60);
        }
        default:
            throw_consistency_error("parsing Run", "style is " + formatStyle(s) + " but we can only decode MINUTE");
    }
}

unique_ptr<Run> Run::decodeString(const std::string& val)
{
    std::string inner;
    Run::Style style = outerParse<Run>(val, inner);
    switch (style)
    {
        case Style::MINUTE: {
			size_t sep = inner.find(':');
			int hour, minute;
			if (sep == string::npos)
			{
				// 12
				hour = strtoul(inner.c_str(), 0, 10);
				minute = 0;
			} else {
				// 12:00
				hour = strtoul(inner.substr(0, sep).c_str(), 0, 10);
				minute = strtoul(inner.substr(sep+1).c_str(), 0, 10);
			}
            return createMinute(hour, minute);
        }
        default:
            throw_consistency_error("parsing Run", "unknown Run style " + formatStyle(style));
    }
}

std::unique_ptr<Run> Run::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::MINUTE: return upcast<Run>(run::Minute::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown Run style");
    }
}

unique_ptr<Run> Run::createMinute(unsigned int hour, unsigned int minute)
{
    return upcast<Run>(run::Minute::create(hour, minute));
}

namespace run {

Run::Style Minute::style() const { return Style::MINUTE; }

void Minute::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Run::encodeWithoutEnvelope(enc);
    enc.add_varint(m_minute);
}
std::ostream& Minute::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed
			 << setw(2) << (m_minute / 60) << ":"
			 << setw(2) << (m_minute % 60) << ")";
}
void Minute::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Run::serialise_local(e, keys, f);
    e.add(keys.run_value, (int)m_minute);
}

std::unique_ptr<Minute> Minute::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    unsigned int m = val.as_int(keys.run_value, "run value");
    return run::Minute::create(m / 60, m % 60);
}

std::string Minute::exactQuery() const
{
	stringstream res;
	res << "MINUTE," << setfill('0') << setw(2) << (m_minute/60) << ":" << setw(2) << (m_minute % 60);
	return res.str();
}

int Minute::compare_local(const Run& o) const
{
	// We should be the same kind, so upcast
	const Minute* v = dynamic_cast<const Minute*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Run, but is a ") + typeid(&o).name() + " instead");

	return m_minute - v->m_minute;
}

bool Minute::equals(const Type& o) const
{
	const Minute* v = dynamic_cast<const Minute*>(&o);
	if (!v) return false;
	return m_minute == v->m_minute;
}

Minute* Minute::clone() const
{
    Minute* res = new Minute;
    res->m_minute = m_minute;
    return res;
}

unique_ptr<Minute> Minute::create(unsigned int hour, unsigned int minute)
{
    Minute* res = new Minute;
    res->m_minute = hour * 60 + minute;
    return unique_ptr<Minute>(res);
}

}

void Run::init()
{
    MetadataType::register_type<Run>();
}

}
}
#include <arki/types/styled.tcc>
