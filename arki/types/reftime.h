#ifndef ARKI_TYPES_REFTIME_H
#define ARKI_TYPES_REFTIME_H

/*
 * types/reftime - Vertical reftime or layer
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
#include <arki/types/time.h>

struct lua_State;

namespace arki {
namespace types {

struct Reftime;

template<>
struct traits<Reftime>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;

};

namespace reftime {
struct Position;
struct Period;
}
template<> struct traits<reftime::Position> : public traits<Reftime> {};
template<> struct traits<reftime::Period> : public traits<Reftime> {};

/**
 * The vertical reftime or layer of some data
 *
 * It can contain information like reftimetype and reftime value.
 */
struct Reftime : public StyledType<Reftime>
{
	/// Style values
	static const Style POSITION = 1;
	static const Style PERIOD = 2;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	/// CODEC functions
	static Item<Reftime> decode(const unsigned char* buf, size_t len);
	static Item<Reftime> decodeString(const std::string& val);
	static Item<Reftime> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);
};

namespace reftime {

struct Position : public Reftime
{
	Item<types::Time> time;

	Position(const Item<types::Time>& time);

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Reftime& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<Position> create(const Item<types::Time>& position);
	static Item<Position> decodeMapping(const emitter::memory::Mapping& val);
};

struct Period : public Reftime
{
	Item<types::Time> begin;
	Item<types::Time> end;

	Period(const Item<types::Time>& begin, const Item<types::Time>& end);

	/**
	 * Convert the reference time interval in an open ended interval.
	 *
	 * If the reference time interval ends sooner than 'seconds' seconds ago,
	 * then it will be converted into 'now'.
	 *
	 * Returns true if the end time has been converted
	 */
	bool setEndtimeToNow(int secondsAgo);

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Reftime& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<Period> create(const Item<types::Time>& begin, const Item<types::Time>& end);
	static Item<Period> decodeMapping(const emitter::memory::Mapping& val);
};

struct Collector
{
	UItem<types::Time> begin;
	UItem<types::Time> end;

	void clear();

	int compare(const Collector& o) const;
	bool operator==(const Collector& c) const;
	bool operator<(const Collector& c) const;
	
	/**
	 * Merge in information from a Reftime
	 */
	void mergeTime(const Item<types::Time>& t);
	void mergeTime(const Item<types::Time>& tbegin, const Item<types::Time>& tend);
	void merge(const Reftime* rt1);
	void merge(const Collector& c);

	/// Create a reftime with the data we have collected
	Item<Reftime> makeReftime() const;
};

}

}
}

// vim:set ts=4 sw=4:
#endif
