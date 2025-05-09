#ifndef ARKI_CORE_TIME_H
#define ARKI_CORE_TIME_H

#include <arki/core/fwd.h>
#include <cstdint>
#include <ctime>
#include <vector>
#include <string>
#include <functional>
#include <iosfwd>

namespace arki {
namespace core {

class TimeBase
{
public:
    /// Year
    int ye = 0;

    /// Month
    int mo = 0;

    /// Day
    int da = 0;

    /// Hour
    int ho = 0;

    /// Minute
    int mi = 0;

    /// Second
    int se = 0;

    TimeBase() = default;
    TimeBase(const TimeBase&) = default;
    TimeBase(TimeBase&&) = default;
    TimeBase(int ye, int mo, int da, int ho, int mi, int se)
        : ye(ye), mo(mo), da(da), ho(ho), mi(mi), se(se) {}
    TimeBase& operator=(const TimeBase&) = default;
    TimeBase& operator=(TimeBase&&) = default;

    /// A time from a struct tm
    TimeBase(struct tm& t);

    /// Return true if the time value has been set to non-zero
    bool is_set() const;

    /// Set from a struct tm
    void set_tm(struct tm& t);

    /// Set from a ISO8601 string
    void set_iso8601(const std::string& str);

    /// Set from a SQL string
    void set_sql(const std::string& str);

    /// Set with the current date and time
    void set_now();

    /// Set to 00:00:00 on easter day
    void set_easter(int year);

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

    /**
     * Create with the datetime one gets by replacing all -1 values with their
     * lowest possible value
     */
    static Time create_lowerbound(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1);

    /**
     * Create with the datetime one gets by replacing all -1 values with their
     * highest possible value
     */
    static Time create_upperbound(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1);

    /// Check if the time is set
    bool isset() const;

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
    /// Return the time at the start of the previous month
    Time start_of_previous_month() const;
    /// Return the time at the very end of this month
    Time end_of_month() const;

    /// Return the time one second before this one
    Time prev_instant() const;
    /// Return the time one second after this one
    Time next_instant() const;

    /// Check if this time is the beginning of a month
    bool is_start_of_month() const;

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

    /**
     * Return the time as a time_t value.
     *
     * Values not representable as time_t will be rendered with the closest
     * time_t value. This means that a value from 1968 will appear as
     * 1970-01-01
     */
    time_t to_unix() const;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const;
    /// Encode in binary format in a preallocated buffer, which must be at least
    /// 5 bytes long
    void encode_binary(uint8_t* buf) const;
    static Time decode(BinaryDecoder& dec);
    static Time decodeString(const std::string& val);

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
     * start points are considered included in the range.
     * end points are considered not included in the range.
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
     * Return the number of seconds between `begin` (included) and `until` (excluded)
     */
    static long long int duration(const Time& begin, const Time& until);

    /**
     * Return the number of seconds between `begin` (included) and `until` (excluded)
     */
    static long long int duration(const core::Interval& interval);
};

static inline std::ostream& operator<<(std::ostream& o, const Time& i)
{
    return o << i.to_iso8601();
}


/**
 * An interval between two moments in time.
 *
 * The point at the start is included in the interval; the point at the end is
 * excluded from the interval.
 *
 * Intervals can be open ended on either or both sides, and open ended extremes
 * are represented as Time() instances, that is, with all values set to zero.
 */
struct Interval
{
    Time begin;
    Time end;

    Interval() = default;
    Interval(const Time& begin, const Time& end);
    Interval(const Interval&) = default;
    Interval(Interval&&) = default;
    Interval& operator=(const Interval&) = default;
    Interval& operator=(Interval&&) = default;

    bool operator==(const Interval& other) const
    {
        return begin == other.begin && end == other.end;
    }

    bool operator!=(const Interval& other) const
    {
        return begin != other.begin || end != other.end;
    }

    /// Return true if the interval has no begin and end bounds
    bool is_unbounded() const;

    /// Check if time is contained in this interval
    bool contains(const Time& time) const;

    /// Check if interval is fully contained in this interval
    bool contains(const Interval& interval) const;

    /// Check if interval is partially contained in this interval
    bool intersects(const Interval& interval) const;

    /// Check if the interval spans one whole month
    bool spans_one_whole_month() const;

    /// Call the function for each (full or partial) month in the interval
    void iter_months(std::function<bool(const Interval&)>) const;

    /**
     * Set this interval as the shortest common interval betwen this and ``o``,
     * and return true.
     *
     * If the two intervals are disjoint, it does nothing and returns false.
     */
    bool intersect(const Interval& other);

    /**
     * Set this interval as the smallest interval that contains both this and
     * ``other``.
     */
    void extend(const Interval& o);

    /// Return a string representation
    std::string to_string() const;
};


static inline std::ostream& operator<<(std::ostream& o, const Interval& i)
{
    return o << i.to_string();
}

}
}
#endif
