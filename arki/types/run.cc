/*
 * types/run - Daily run identification for a periodic data source
 *
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <arki/types/run.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_RUN
#define TAG "run"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

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
	throw wibble::exception::Consistency("parsing Run style", "cannot parse Run style '"+str+"': only MINUTE is supported");
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

Item<Run> Run::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "run style");
	switch (s)
	{
		case MINUTE: {
		        unsigned int m = dec.popVarint<unsigned>("run minute");
			return run::Minute::create(m / 60, m % 60);
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "style is " + formatStyle(s) + " but we can only decode MINUTE");
	}
}
    
Item<Run> Run::decodeString(const std::string& val)
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
			return run::Minute::create(hour, minute);
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "unknown Run style " + formatStyle(style));
	}
}

namespace run {

static TypeCache<Minute> cache_minute;

Run::Style Minute::style() const { return Run::MINUTE; }

void Minute::encodeWithoutEnvelope(Encoder& enc) const
{
	Run::encodeWithoutEnvelope(enc);
	enc.addVarint(m_minute);
}
std::ostream& Minute::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed
			 << setw(2) << (m_minute / 60) << ":"
			 << setw(2) << (m_minute % 60) << ")";
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
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Run, but is a ") + typeid(&o).name() + " instead");

	return m_minute - v->m_minute;
}

bool Minute::operator==(const Type& o) const
{
	const Minute* v = dynamic_cast<const Minute*>(&o);
	if (!v) return false;
	return m_minute == v->m_minute;
}

Item<Minute> Minute::create(unsigned int hour, unsigned int minute)
{
	Minute* res;
	Item<Minute> itemres = res = new Minute;
	res->m_minute = hour * 60 + minute;
	return cache_minute.intern(itemres);
}

static void debug_interns()
{
	fprintf(stderr, "Run Minute: sz %zd reused %zd\n", cache_minute.size(), cache_minute.reused());
}

}

static MetadataType runType = MetadataType::create<Run>(run::debug_interns);

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
