#ifndef ARKI_TYPES_TIMERANGE_H
#define ARKI_TYPES_TIMERANGE_H

#include <arki/types.h>
#include <arki/types/reftime.h>
#include <arki/types/encoded.h>
#include <stdint.h>

namespace arki {
namespace types {
namespace timerange {

/// Style values
enum class Style: unsigned char {
    GRIB1 = 1,
    GRIB2 = 2,
    BUFR = 3,
    TIMEDEF = 4,
};

enum GRIB1Unit {
    SECOND = 0,
    MONTH = 1
};

enum TimedefUnit {
    UNIT_MINUTE  = 0,
    UNIT_HOUR    = 1,
    UNIT_DAY     = 2,
    UNIT_MONTH   = 3,
    UNIT_YEAR    = 4,
    UNIT_DECADE  = 5,
    UNIT_NORMAL  = 6,
    UNIT_CENTURY = 7,
    UNIT_3HOURS  = 10,
    UNIT_6HOURS  = 11,
    UNIT_12HOURS = 12,
    UNIT_SECOND  = 13,
    UNIT_MISSING = 255
};

}

template<>
struct traits<Timerange>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef timerange::Style Style;
};

template<>
struct traits<timerange::Timedef>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef timerange::Style Style;
};

/**
 * The time span information of the data
 *
 * It can contain information such as accumulation time, or validity of
 * forecast.
 */
struct Timerange : public types::Encoded
{
public:
    using Encoded::Encoded;

    typedef timerange::Style Style;

    ~Timerange();

    types::Code type_code() const override { return traits<Timerange>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Timerange>::type_sersize_bytes; }
    std::string tag() const override { return traits<Timerange>::type_tag; }

    Timerange* clone() const override = 0;

    int compare(const Type& o) const override;

    // Get the element style
    static timerange::Style style(const uint8_t* data, unsigned size);
    static void get_GRIB1(const uint8_t* data, unsigned size, unsigned& type, unsigned& unit, unsigned& p1, unsigned& p2);
    static void get_GRIB1_normalised(const uint8_t* data, unsigned size, int& type, timerange::GRIB1Unit& unit, int& p1, int& p2, bool& use_op1, bool& use_op2);

    static void get_GRIB2(const uint8_t* data, unsigned size, unsigned& type, unsigned& unit, signed long& p1, signed long& p2);
    static void get_Timedef(const uint8_t* data, unsigned size, timerange::TimedefUnit& step_unit, unsigned& step_len, unsigned& stat_type, timerange::TimedefUnit& stat_unit, unsigned& stat_len);
    static void get_BUFR(const uint8_t* data, unsigned size, unsigned& unit, unsigned& value);

    timerange::Style style() const { return style(data, size); }
    void get_GRIB1(unsigned& type, unsigned& unit, unsigned& p1, unsigned& p2) const
    {
        get_GRIB1(data, size, type, unit, p1, p2);
    }
    void get_GRIB1_normalised(int& type, timerange::GRIB1Unit& unit, int& p1, int& p2, bool& use_op1, bool& use_op2) const
    {
        get_GRIB1_normalised(data, size, type, unit, p1, p2, use_op1, use_op2);
    }
    void get_GRIB2(unsigned& type, unsigned& unit, signed long& p1, signed long& p2) const
    {
        get_GRIB2(data, size, type, unit, p1, p2);
    }
    void get_Timedef(timerange::TimedefUnit& step_unit, unsigned& step_len, unsigned& stat_type, timerange::TimedefUnit& stat_unit, unsigned& stat_len) const
    {
        get_Timedef(data, size, step_unit, step_len, stat_type, stat_unit, stat_len);
    }
    void get_BUFR(unsigned& unit, unsigned& value) const
    {
        get_BUFR(data, size, unit, value);
    }

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
    static std::unique_ptr<Timerange> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Timerange> decodeString(const std::string& val);
    static std::unique_ptr<Timerange> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Timerange> createGRIB1(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2);
    static std::unique_ptr<Timerange> createGRIB2(unsigned char type, unsigned char unit, signed long p1, signed long p2);
    static std::unique_ptr<Timerange> createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit=timerange::UNIT_SECOND);
    static std::unique_ptr<Timerange> createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit,
                                                  uint8_t stat_type, uint32_t stat_len, timerange::TimedefUnit stat_unit=timerange::UNIT_SECOND);
    static std::unique_ptr<Timerange> createBUFR(unsigned value = 0, unsigned char unit = 254);
};

namespace timerange {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Timerange::formatStyle(s); }


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
    //bool get_timeunit_conversion(int& timemul) const;

public:
    using Timerange::Timerange;

    GRIB1* clone() const override
    {
        return new GRIB1(data, size);
    }

    bool equals(const Type& o) const override;
    int compare_local(const GRIB1& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    static void arg_significance(unsigned type, bool& use_p1, bool& use_p2);
};

class GRIB2 : public Timerange
{
public:
    using Timerange::Timerange;

    GRIB2* clone() const override
    {
        return new GRIB2(data, size);
    }

    int compare_local(const GRIB2& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;
};

class Timedef : public Timerange
{
public:
    using Timerange::Timerange;

    Timedef* clone() const override
    {
        return new Timedef(data, size);
    }

    bool equals(const Type& o) const override;
    int compare_local(const Timedef& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    /**
     * Given a reftime representing validity time, compute and return its
     * emission time, shifting it by what is represented by this timedef
     */
    // TODO: take a core::Time instead of a reftime::Position
    std::unique_ptr<Reftime> validity_time_to_emission_time(const reftime::Position& src) const;

    // static std::unique_ptr<Timedef> createFromYaml(const std::string& str);

    /**
     * Unit conversion for code table 4.4 GRIB2 indicator of unit of time range
     *
     * @param unit value to use
     * @retval timemul
     *   Factor to multiply to a value in the current units to obtain months or
     *   seconds
     * @returns
     *   true if multiplying by timemul gives seconds, false if it gives months
     */
    static bool timeunit_conversion(TimedefUnit unit, int& timemul);

    /**
     * Return the suffix for the given time unit
     *
     * Can return NULL if unit is 255 or an invalid/unsupported value
     */
    static const char* timeunit_suffix(TimedefUnit unit);

    static void timeunit_output(TimedefUnit unit, uint32_t val, std::ostream& out);

    static bool timeunit_parse_suffix(const char*& str, TimedefUnit& unit);
    static bool timeunit_parse(const char*& str, TimedefUnit& unit, uint32_t& val);
};

class BUFR : public Timerange
{
public:
    using Timerange::Timerange;

    BUFR* clone() const override
    {
        return new BUFR(data, size);
    }

    bool equals(const Type& o) const override;
    int compare_local(const BUFR& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    static bool is_seconds(unsigned unit);
    static unsigned seconds(unsigned unit, unsigned value);
    static unsigned months(unsigned unit, unsigned value);
};

}
}
}
#endif
