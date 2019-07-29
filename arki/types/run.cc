#include "arki/exceptions.h"
#include "arki/types/run.h"
#include "arki/types/utils.h"
#include "arki/utils/iostream.h"
#include "arki/binary.h"
#include "arki/emitter.h"
#include "arki/emitter/memory.h"
#include "arki/libconfig.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif

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
const char* traits<Run>::type_lua_tag = LUATAG_TYPES ".run";

// Style constants
//const unsigned char Run::NONE;
const unsigned char Run::MINUTE;

Run::Style Run::parseStyle(const std::string& str)
{
	if (str == "MINUTE") return MINUTE;
	throw_consistency_error("parsing Run style", "cannot parse Run style '"+str+"': only MINUTE is supported");
}

std::string Run::formatStyle(Run::Style s)
{
	switch (s)
	{
		//case Run::NONE: return "NONE";
		case Run::MINUTE: return "MINUTE";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<Run> Run::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "run style");
    switch (s)
    {
        case MINUTE: {
            unsigned int m = dec.pop_varint<unsigned>("run minute");
            return createMinute(m / 60, m % 60);
        }
        default:
            throw_consistency_error("parsing Run", "style is " + formatStyle(s) + " but we can only decode MINUTE");
    }
}

unique_ptr<Run> Run::decodeString(const std::string& val)
{
	string inner;
	Run::Style style = outerParse<Run>(val, inner);
	switch (style)
	{
		case Run::MINUTE: {
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

unique_ptr<Run> Run::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Run::MINUTE: return upcast<Run>(run::Minute::decodeMapping(val));
        default:
            throw_consistency_error("parsing Run", "unknown Run style " + val.get_string());
    }
}

#ifdef HAVE_LUA
static int arkilua_new_minute(lua_State* L)
{
	int nargs = lua_gettop(L);
	int hour = luaL_checkint(L, 1);
	if (nargs == 1)
	{
		run::Minute::create(hour, 0)->lua_push(L);
	} else {
		int minute = luaL_checkint(L, 2);
		run::Minute::create(hour, minute)->lua_push(L);
	}
	return 1;
}

void Run::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "minute", arkilua_new_minute },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_run", lib);
}
#endif

unique_ptr<Run> Run::createMinute(unsigned int hour, unsigned int minute)
{
    return upcast<Run>(run::Minute::create(hour, minute));
}

namespace run {

Run::Style Minute::style() const { return Run::MINUTE; }

void Minute::encodeWithoutEnvelope(BinaryEncoder& enc) const
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
void Minute::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Run::serialiseLocal(e, f);
    e.add("va", (int)m_minute);
}
unique_ptr<Minute> Minute::decodeMapping(const emitter::memory::Mapping& val)
{
    unsigned int m = val["va"].want_int("parsing Minute run value");
    return run::Minute::create(m / 60, m % 60);
}
std::string Minute::exactQuery() const
{
	stringstream res;
	res << "MINUTE," << setfill('0') << setw(2) << (m_minute/60) << ":" << setw(2) << (m_minute % 60);
	return res.str();
}
const char* Minute::lua_type_name() const { return "arki.types.run.minute"; }

#ifdef HAVE_LUA
bool Minute::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "hour")
		lua_pushnumber(L, minute() / 60);
	else if (name == "min")
		lua_pushnumber(L, minute() % 60);
	else
		return Run::lua_lookup(L, name);
	return true;
}
#endif


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
#include <arki/types.tcc>
