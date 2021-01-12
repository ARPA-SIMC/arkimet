#ifndef ARKI_MATCHER_TIMERANGE
#define ARKI_MATCHER_TIMERANGE

#include <arki/matcher/utils.h>
#include <arki/types/timerange.h>

namespace arki {
namespace matcher {

/**
 * Match Timeranges
 *
 * Syntax: GRIB,1,0,0 or GRIB,1 or GRIB,1,, instant
 *         GRIB,2,0,3h forecast next 3 hours
 *         GRIB,3 any average
 */
struct MatchTimerange : public Implementation
{
    MatchTimerange* clone() const override = 0;
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchTimerangeGRIB1 : public MatchTimerange
{
    types::timerange::GRIB1Unit unit;
    Optional<int> ptype;
    Optional<int> p1;
    Optional<int> p2;

    MatchTimerangeGRIB1(types::timerange::GRIB1Unit unit, const Optional<int>& ptype, const Optional<int>& p1, const Optional<int>& p2);
    MatchTimerangeGRIB1(const std::string& pattern);
    MatchTimerangeGRIB1* clone() const override;
    bool match_data(int mtype, int munit, int mp1, int mp2, bool use_p1, bool use_p2) const;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchTimerangeGRIB2 : public MatchTimerange
{
	int type;
	int unit;
	int p1;
	int p2;

    MatchTimerangeGRIB2(int type, int unit, int p1, int p2);
    MatchTimerangeGRIB2(const std::string& pattern);
    MatchTimerangeGRIB2* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchTimerangeBUFR : public MatchTimerange
{
    Optional<unsigned> forecast;
    bool is_seconds;

    MatchTimerangeBUFR(const Optional<unsigned>& forecast, bool is_seconds);
    MatchTimerangeBUFR(const std::string& pattern);
    MatchTimerangeBUFR* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

/**
 * 'timedef' timerange matcher implementation.
 *
 * Syntax:
 *
 *    timerange:timedef,+72h,1,6h
 *                        |  | +---- Duration of statistical process
 *                        |  +------ Type of statistical process (from DB-All.e)
 *                        +--------- Forecast step
 */
struct MatchTimerangeTimedef : public MatchTimerange
{
    Optional<int> step;
    bool step_is_seconds = true;

    Optional<int> proc_type;

    Optional<int> proc_duration;
    bool proc_duration_is_seconds = true;

    MatchTimerangeTimedef(const Optional<int>& step, bool step_is_seconds, const Optional<int>& proc_type, const Optional<int>& proc_duration, bool proc_duration_is_seconds);
    MatchTimerangeTimedef(const std::string& pattern);
    MatchTimerangeTimedef* clone() const override;
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}
#endif
