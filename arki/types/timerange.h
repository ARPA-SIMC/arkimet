#ifndef ARKI_TYPES_TIMERANGE_H
#define ARKI_TYPES_TIMERANGE_H

/*
 * types/timerange - Time span information
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

struct Timerange;

template<>
struct traits<Timerange>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The time span information of the data
 *
 * It can contain information such as accumulation time, or validity of
 * forecast.
 */
struct Timerange : public types::StyledType<Timerange>
{
	/// Style values
	static const Style GRIB1 = 1;
	static const Style GRIB2 = 2;
	static const Style BUFR = 3;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /**
     * Compute the forecast step
     *
     * @retval step The forecast step
     * @retval is_seconds if true, the forecast step is in seconds, if false it
     *         is in months
     * @return true if the forecast step could be computed, else false
     */
    virtual bool get_forecast_step(int& step, bool& is_seconds) const = 0;

    /**
     * Return the type of statistical processing (or -1 if not available)
     */
    virtual int get_proc_type() const = 0;

    /**
     * Compute the duration of statistical processing
     *
     * @retval duration The computed duration
     * @retval is_seconds if true, the duration is in seconds, if false it is
     *         in months
     * @return true if the duration could be computed, else false
     */
    virtual bool get_proc_duration(int& duration, bool& is_seconds) const = 0;

	/// CODEC functions
	static Item<Timerange> decode(const unsigned char* buf, size_t len);
	static Item<Timerange> decodeString(const std::string& val);
	static Item<Timerange> decodeMapping(const emitter::memory::Mapping& val);

	static void lua_loadlib(lua_State* L);
};

namespace timerange {

struct GRIB1 : public Timerange
{
protected:
	std::ostream& writeNumbers(std::ostream& o) const;
    /**
     * Get time unit conversion
     *
     * @retval timemul
     *   Factor to multiply to a value in the current units to obtain months or
     *   seconds
     * @returns
     *   true if multiplying by timemul gives seconds, false if it gives months
     */
    bool get_timeunit_conversion(int& timemul) const;

	unsigned char m_type, m_unit;
	unsigned char m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	unsigned p1() const { return m_p1; }
	unsigned p2() const { return m_p2; }

	enum Unit {
		SECOND = 0,
		MONTH = 1
	};

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    virtual bool get_forecast_step(int& step, bool& is_seconds) const;
    virtual int get_proc_type() const;
    virtual bool get_proc_duration(int& duration, bool& is_seconds) const;

	virtual int compare_local(const Timerange& o) const;
	virtual bool operator==(const Type& o) const;

	void getNormalised(int& type, Unit& unit, int& p1, int& p2) const;

	static Item<GRIB1> create(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2);
	static Item<GRIB1> decodeMapping(const emitter::memory::Mapping& val);
};

class GRIB2 : public Timerange
{
protected:
	unsigned char m_type, m_unit;
	signed long m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	signed p1() const { return m_p1; }
	signed p2() const { return m_p2; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    virtual bool get_forecast_step(int& step, bool& is_seconds) const;
    virtual int get_proc_type() const;
    virtual bool get_proc_duration(int& duration, bool& is_seconds) const;

	virtual int compare_local(const Timerange& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2> create(unsigned char type, unsigned char unit, signed long p1, signed long p2);
	static Item<GRIB2> decodeMapping(const emitter::memory::Mapping& val);
};

class BUFR : public Timerange
{
protected:
	unsigned char m_unit;
	unsigned m_value;

public:
	unsigned unit() const { return m_unit; }
	unsigned value() const { return m_value; }

	bool is_seconds() const;
	unsigned seconds() const;
	unsigned months() const;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    virtual bool get_forecast_step(int& step, bool& is_seconds) const;
    virtual int get_proc_type() const;
    virtual bool get_proc_duration(int& duration, bool& is_seconds) const;

	virtual int compare_local(const Timerange& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<BUFR> create(unsigned value = 0, unsigned char unit = 254);
	static Item<BUFR> decodeMapping(const emitter::memory::Mapping& val);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
