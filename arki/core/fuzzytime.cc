#include "fuzzytime.h"
#include <cstring>

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

}
}
