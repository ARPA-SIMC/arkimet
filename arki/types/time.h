#ifndef ARKI_TYPES_TIME_H
#define ARKI_TYPES_TIME_H

#include <arki/types.h>

struct lua_State;

namespace arki {

namespace emitter {
namespace memory {
struct List;
}
}

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
class Time : public types::CoreType<Time>
{
public:
    int vals[6];

    /// An uninitialized time
    Time();
    Time(int ye, int mo, int da, int ho=0, int mi=0, int se=0);
    Time(const int (&vals)[6]);
    Time(const Time& t);
    Time(struct tm& t);

	Time& operator=(const Time& t);

    int compare_raw(const int (&vals)[6]) const;
    int compare(const Type& o) const override;
    bool equals(const Type& t) const override;

    /// Some time operations

    /// Return the time at the start of this month
    Time start_of_month() const;
    /// Return the time at the start of the next month
    Time start_of_next_month() const;
    /// Return the time at the very end of this month
    Time end_of_month() const;

	/// Return the time formatted as a string in ISO-8601 format
	std::string toISO8601(char sep='T') const;

	/// Return the time formatted as a string in SQL format
	std::string toSQL() const;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    static std::unique_ptr<Time> decode(BinaryDecoder& dec);
    static std::unique_ptr<Time> decodeString(const std::string& val);
    static std::unique_ptr<Time> decodeMapping(const emitter::memory::Mapping& val);
    static std::unique_ptr<Time> decodeList(const emitter::memory::List& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    void serialiseList(Emitter& e) const;

    // Lua functions
    virtual void lua_register_methods(lua_State* L) const override;

    Time* clone() const override;

    void set(int ye, int mo, int da, int ho, int mi, int se);
    void set(const int (&vals)[6]);
    void set(struct tm& t);
    void setFromISO8601(const std::string& str);
    void setFromSQL(const std::string& str);
    void setNow();

    /// Construct a Time for the given date
    static std::unique_ptr<Time> create(int ye, int mo, int da, int ho, int mi, int se);

    /// Construct a Time for the given date, as an array of 6 ints
    static std::unique_ptr<Time> create(const int (&vals)[6]);

    /// Construct a Time from a struct tm
    static std::unique_ptr<Time> create(struct tm& t);

    /// Create a Time object from a string in ISO-8601 format
    static std::unique_ptr<Time> createFromISO8601(const std::string& str);

    /// Create a Time object from a string in SQL format
    static Time create_from_SQL(const std::string& str);

    /// Create a Time object with the current time
    static Time createNow();

    /**
     * Generate a sequence of Position reftime values.
     *
     * The sequence starts at \a begin (included) and ends at \a end
     * (excluded). Element are \a step seconds apart.
     */
    static std::vector<Time> generate(
            const types::Time& begin, const types::Time& end, int step);

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


	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

static inline std::ostream& operator<<(std::ostream& o, const Time& i)
{
    return i.writeToOstream(o);
}


}
}
#endif
