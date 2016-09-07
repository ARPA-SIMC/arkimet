#ifndef ARKI_CORE_FUZZYTIME_H
#define ARKI_CORE_FUZZYTIME_H

namespace arki {
namespace core {

/**
 * Represent a partial time.
 *
 * Any FuzzyTime member can be -1, to mean "any". After the first element set
 * to -1, all following elements are ignored and assumed to all be -1.
 */
class FuzzyTime
{
public:
    /// Year
    int ye = -1;

    /// Month
    int mo = -1;

    /// Day
    int da = -1;

    /// Hour
    int ho = -1;

    /// Minute
    int mi = -1;

    /// Second
    int se = -1;

    FuzzyTime() = default;
    FuzzyTime(const FuzzyTime&) = default;
    FuzzyTime(FuzzyTime&&) = default;
    FuzzyTime(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1);
    FuzzyTime& operator=(const FuzzyTime&) = default;
    FuzzyTime& operator=(FuzzyTime&&) = default;
};

}
}

#endif
