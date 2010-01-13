#ifndef ARKI_TYPES_RUN_H
#define ARKI_TYPES_RUN_H

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

#include <arki/types.h>

struct lua_State;

namespace arki {
namespace types {

/**
 * The run of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Run : public types::Type
{
	typedef unsigned char Style;

	/// Style values
	//static const Style NONE = 0;
	static const Style MINUTE = 1;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// Run style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Run& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<Run> decode(const unsigned char* buf, size_t len);
	static Item<Run> decodeString(const std::string& val);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Run
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Run LUA object
	static int lua_lookup(lua_State* L);
};

namespace run {

struct Minute : public Run
{
	unsigned int minute;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;

	virtual int compare(const Run& o) const;
	virtual int compare(const Minute& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<Minute> create(unsigned int hour, unsigned int minute = 0);
};

}

}
}

#undef ARKI_GEOS_GEOMETRY

// vim:set ts=4 sw=4:
#endif
