#include "fuzzytime.h"

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

}
}
