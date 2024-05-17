#include "fuzzytime.h"
#include <arki/core/time.h>
#include <stdexcept>
#include <cstring>
#include <cstdio>

namespace arki {
namespace core {

Time FuzzyTime::lowerbound()
{
    Time res;
    res.set_lowerbound(ye, mo, da, ho, mi, se);
    return res;
}

Time FuzzyTime::upperbound()
{
    Time res;
    res.set_upperbound(ye, mo, da, ho, mi, se);
    return res;
}

void FuzzyTime::set_easter(int year)
{
    TimeBase::set_easter(year);
    ho = mi = se = -1;
}

std::string FuzzyTime::to_string() const
{
    char buf[25];
    char* s = buf;

    if (ye == -1)
        memcpy(s, "-----", 5);
    else
        snprintf(s, 6, "%04d-", ye);
    s += 5;

    if (mo == -1)
        memcpy(s, "---", 3);
    else
        snprintf(s, 4, "%02d-", mo);
    s += 3;

    if (da == -1)
        memcpy(s, "-- ", 3);
    else
        snprintf(s, 4, "%02d ", da);
    s += 3;

    if (ho == -1)
        memcpy(s, "--:", 3);
    else
        snprintf(s, 4, "%02d:", ho);
    s += 3;

    if (mi == -1)
        memcpy(s, "--:", 3);
    else
        snprintf(s, 4, "%02d:", mi);
    s += 3;

    if (se == -1)
        memcpy(s, "--", 3);
    else
        snprintf(s, 3, "%02d", se);

    return buf;
}

static void check_minmax(int value, int min, int max, const char* what)
{
    if (value < min || value > max)
        throw std::invalid_argument(
            std::string(what) + " must be between "
            + std::to_string(min) + " and " + std::to_string(max));
}

void FuzzyTime::validate() const
{
    if (mo != -1) check_minmax(mo, 1, 12, "month");
    if (ho != -1) check_minmax(ho, 0, 24, "hour");
    if (mi != -1) check_minmax(mi, 0, 59, "minute");
    if (se != -1) check_minmax(se, 0, 60, "second");

    if (mo != -1 && da != -1)
        check_minmax(da, 1, Time::days_in_month(ye, mo), "day");

    if (ho == 24)
    {
        if (mi != -1 && mi != 0)
            throw std::invalid_argument("on hour 24, minute must be zero");
        if (se != -1 && se != 0)
            throw std::invalid_argument("on hour 24, second must be zero");
    }
}

}
}
