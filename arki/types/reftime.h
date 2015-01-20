#ifndef ARKI_TYPES_REFTIME_H
#define ARKI_TYPES_REFTIME_H

/*
 * types/reftime - Vertical reftime or layer
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
    static std::auto_ptr<Reftime> decode(const unsigned char* buf, size_t len);
    static std::auto_ptr<Reftime> decodeString(const std::string& val);
    static std::auto_ptr<Reftime> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);

    /// Beginning of the period in this Reftime
    virtual const Time& period_begin() const = 0;
    /// End of the period in this Reftime
    virtual const Time& period_end() const = 0;

    /**
     * Expand a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * A NULL auto_ptr signifies the initial state of an invalid range, and
     * both begin and end will be set to non-NULL as soon as the first
     * expand_date_range is called on them.
     */
    virtual void expand_date_range(std::auto_ptr<types::Time>& begin, std::auto_ptr<types::Time>& end) const = 0;

    /**
     * Expand a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * begin and end are assumed to be valid times.
     */
    virtual void expand_date_range(types::Time& begin, types::Time& end) const = 0;

    // Register this type tree with the type system
    static void init();

    /// If begin == end create a Position reftime, else create a Period reftime
    static std::auto_ptr<Reftime> create(const Time& begin, const Time& end);
    static std::auto_ptr<Reftime> createPosition(const Time& position);
    static std::auto_ptr<Reftime> createPeriod(const Time& begin, const Time& end);
};

namespace reftime {

struct Position : public Reftime
{
    Time time;

    Position(const Time& time);

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Reftime& o) const override;
    bool equals(const Type& o) const override;

    const Time& period_begin() const override { return time; }
    const Time& period_end() const override { return time; }

    Position* clone() const override;

    void expand_date_range(std::auto_ptr<types::Time>& begin, std::auto_ptr<types::Time>& end) const override;
    void expand_date_range(types::Time& begin, types::Time& end) const override;

    static std::auto_ptr<Position> create(const Time& position);
    static std::auto_ptr<Position> decodeMapping(const emitter::memory::Mapping& val);
};

struct Period : public Reftime
{
    Time begin;
    Time end;

    Period(const Time& begin, const Time& end);

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Reftime& o) const override;
    bool equals(const Type& o) const override;

    const Time& period_begin() const override { return begin; }
    const Time& period_end() const override { return end; }

    Period* clone() const override;

    void expand_date_range(std::auto_ptr<types::Time>& begin, std::auto_ptr<types::Time>& end) const override;
    void expand_date_range(types::Time& begin, types::Time& end) const override;

    static std::auto_ptr<Period> create(const Time& begin, const Time& end);
    static std::auto_ptr<Period> decodeMapping(const emitter::memory::Mapping& val);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
