#ifndef ARKI_TYPES_TIME_H
#define ARKI_TYPES_TIME_H

/*
 * types/time - Time
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

struct Time;

template<>
struct traits<Time>
{
        static const char* type_tag;
        static const types::Code type_code;
        static const size_t type_sersize_bytes;
        static const char* type_lua_tag;
};

/**
 * A point in time, in UTC.
 *
 * If all the time components are 0, it is to be interpreted as 'now'.
 */
struct Time : public types::CoreType<Time>
{
	int vals[6];

	Time();
	Time(int ye, int mo, int da, int ho, int mi, int se);
	Time(const int (&vals)[6]);
	Time(const Time& t);

	bool isNow() const;

	Time& operator=(const Time& t);

	virtual int compare(const Type& o) const;

	virtual bool operator==(const Type& o) const;
	virtual bool operator==(const Time& o) const;
	virtual bool operator!=(const Time& o) const { return !operator==(o); }

	bool operator<(const Time& o) const { return compare(o) < 0; }
	bool operator<=(const Time& o) const { return compare(o) <= 0; }
	bool operator>(const Time& o) const { return compare(o) > 0; }
	bool operator>=(const Time& o) const { return compare(o) >= 0; }

	const int& operator[](unsigned idx) const { return vals[idx]; }
	int& operator[](unsigned idx) { return vals[idx]; }

	/// Return the time formatted as a string in ISO-8601 format
	std::string toISO8601(char sep='T') const;

	/// Return the time formatted as a string in SQL format
	std::string toSQL() const;

	/// CODEC functions
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<Time> decode(const unsigned char* buf, size_t len);
	static Item<Time> decodeString(const std::string& val);
	std::ostream& writeToOstream(std::ostream& o) const;

	// Lua functions
	virtual void lua_register_methods(lua_State* L) const;

	/// Construct a "now" time of (0, 0, 0, 0, 0, 0)
	static Item<Time> create();

	/// Construct a Time for the given date
	static Item<Time> create(int ye, int mo, int da, int ho, int mi, int se);

	/// Construct a Time for the given date, as an array of 6 ints
	static Item<Time> create(const int (&vals)[6]);

	/// Construct a Time from a struct tm
	static Item<Time> create(struct tm& t);

	/// Create a Time object from a string in ISO-8601 format
	static Item<Time> createFromISO8601(const std::string& str);

	/// Create a Time object from a string in SQL format
	static Item<Time> createFromSQL(const std::string& str);

	/// Create a Time object with the current time
	static Item<Time> createNow();

	/**
	 * Create a Time with the difference between the two times (a - b).
	 *
	 * If a is not greather than b, returns 0000-00-00 00:00:00
	 */
	static Item<Time> createDifference(const Item<Time>& a, const Item<Time>& b);

	/**
	 * Generate a sequence of Position reftime values.
	 *
	 * The sequence starts at \a begin (included) and ends at \a end
	 * (excluded). Element are \a step seconds apart.
	 */
	static std::vector< Item<Time> > generate(
			const types::Time& begin, const types::Time& end, int step);

	static void lua_loadlib(lua_State* L);
};

static inline std::ostream& operator<<(std::ostream& o, const Time& i)
{
    return i.writeToOstream(o);
}


}
}

// vim:set ts=4 sw=4:
#endif
