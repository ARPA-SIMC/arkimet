#ifndef ARKI_CORE_TIME_H
#define ARKI_CORE_TIME_H

#include <vector>
#include <string>

struct lua_State;

namespace arki {
struct BinaryEncoder;
struct BinaryDecoder;
struct Emitter;

namespace emitter {
namespace memory {
struct List;
struct Mapping;
}
}

namespace core {

class TimeBase
{
public:
    /// Year
    int ye;

    /// Month
    int mo;

    /// Day
    int da;

    /// Hour
    int ho;

    /// Minute
    int mi;

    /// Second
    int se;

    TimeBase() = default;
    TimeBase(const TimeBase&) = default;
    TimeBase(TimeBase&&) = default;
    TimeBase(int ye, int mo, int da, int ho, int mi, int se)
        : ye(ye), mo(mo), da(da), ho(ho), mi(mi), se(se) {}
    TimeBase& operator=(const TimeBase&) = default;
    TimeBase& operator=(TimeBase&&) = default;

    /// A time from a struct tm
    TimeBase(struct tm& t);

    /// Set from a struct tm
    void set_tm(struct tm& t);

    /// Set from a ISO8601 string
    void set_iso8601(const std::string& str);

    /// Set from a SQL string
    void set_sql(const std::string& str);

    /// Set with the current date and time
    void set_now();

    int compare(const TimeBase& o) const;
    bool operator<(const TimeBase& o) const { return compare(o) < 0; }
    bool operator<=(const TimeBase& o) const { return compare(o) <= 0; }
    bool operator>(const TimeBase& o) const { return compare(o) > 0; }
    bool operator>=(const TimeBase& o) const { return compare(o) >= 0; }

    bool operator==(const TimeBase& o) const;
    bool operator!=(const TimeBase& o) const;
};


/**
 * A point in time, in UTC.
 *
 * If all the time components are 0, it is to be interpreted as `unset`
 */
class Time : public TimeBase
{
public:
    Time() : TimeBase() {}

    /// A time given its 6 components
    Time(int ye, int mo, int da=1, int ho=0, int mi=0, int se=0)
        : TimeBase(ye, mo, da, ho, mi, se) {}

    Time(struct tm& t) : TimeBase(t) {}

    /// Create a Time object from a string in ISO-8601 format
    static Time create_iso8601(const std::string& str);

    /// Create a Time object from a string in SQL format
    static Time create_sql(const std::string& str);

    /// Create a Time object with the current time
    static Time create_now();

    /// Set all components to 0
    void unset();

    /// Set from the 6 components
    void set(int ye, int mo, int da, int ho=0, int mi=0, int se=0);

    /**
     * Set with the datetime one gets by replacing all -1 values with their
     * lowest possible value
     */
    void set_lowerbound(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1);

    /**
     * Set with the datetime one gets by replacing all -1 values with their
     * highest possible value
     */
    void set_upperbound(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1);

    bool operator==(const TimeBase& o) const { return TimeBase::operator==(o); }

    /// Compare with a stringified version, useful for testing
    bool operator==(const std::string& o) const;

    /// Some time operations

    /// Return the time at the start of this month
    Time start_of_month() const;
    /// Return the time at the start of the next month
    Time start_of_next_month() const;
    /// Return the time at the very end of this month
    Time end_of_month() const;

    /**
     * Normalise out of bound values.
     *
     * This function can be called to normalise a Time after setting some of
     * its components to out of range values.
     *
     * The Time will be normalised so that all the elements will be within
     * range, and it will still represent the same instant.
     *
     * For example (remember that months and days start from 1, so a day of 0 means
     * "last day of previous month"):
     *
     * \l normalising Time(2007, 0, 1) gives Time(2006, 12, 1)
     * \l normalising Time(2007, -11, 1) gives Time(2006, 1, 1)
     * \l normalising Time(2007, 1, -364) gives Time(2006, 1, 1)
     * \l normalising Time(2007, 1, 366) gives Time(2008, 1, 1)
     * \l normalising Time(2009, 1, -364) gives Time(2008, 1, 2), because
     *    2008 is a leap year
     * \l normalising Time(2008, 1, 1, 0, 0, -3600) gives Time(2007, 12, 31, 23)
     */
    void normalise();

    /// Return the time formatted as a string in ISO-8601 format
    std::string to_iso8601(char sep='T') const;

    /// Return the time formatted as a string in SQL format
    std::string to_sql() const;


    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const;
    static Time decode(BinaryDecoder& dec);
    static Time decodeString(const std::string& val);
    static Time decodeMapping(const emitter::memory::Mapping& val);
    static Time decodeList(const emitter::memory::List& val);
    void serialise(Emitter& e) const;
    void serialiseLocal(Emitter& e) const;
    void serialiseList(Emitter& e) const;

    /**
     * Generate a sequence of Position reftime values.
     *
     * The sequence starts at \a begin (included) and ends at \a end
     * (excluded). Element are \a step seconds apart.
     */
    static std::vector<Time> generate(
            const Time& begin, const Time& end, int step);

    /// Return the number of days in a month
    static int days_in_month(int year, int month);

    /// Return the number of days in a year
    static int days_in_year(int year);

    /**
     * Check if two ranges overlap.
     *
     * Ranges can be open ended: an open end is represented by a Time object
     * with all its values set to zero.
     *
     * @param ts1 start of the first range
     * @param te1 end of the first range
     * @param ts2 start of the second range
     * @param te2 end of the second range
     */
    static bool range_overlaps(
            const Time* ts1, const Time* te1,
            const Time* ts2, const Time* te2);

    /**
     * Return the number of seconds between `begin` and `until`
     */
    static long long int duration(const Time& begin, const Time& until);

    /// Push this time on the Lua stack
    void lua_push(lua_State* L) const;

    /// Return the Time at the given position in the Lua stack
    static Time lua_check(lua_State* L, int idx);

    /// Load Time functions in the Lua stack
    static void lua_loadlib(lua_State* L);
};

static inline std::ostream& operator<<(std::ostream& o, const Time& i)
{
    return o << i.to_iso8601();
}


}
}
#endif
