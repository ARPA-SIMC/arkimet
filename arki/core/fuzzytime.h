#ifndef ARKI_CORE_FUZZYTIME_H
#define ARKI_CORE_FUZZYTIME_H

#include <arki/core/time.h>
#include <string>

namespace arki {
namespace core {

/**
 * Represent a partial time.
 *
 * Any FuzzyTime member can be -1, to mean "any". After the first element set
 * to -1, all following elements are ignored and assumed to all be -1.
 */
class FuzzyTime : public TimeBase
{
public:
    using TimeBase::TimeBase;
    using TimeBase::operator=;

    FuzzyTime() : TimeBase(-1, -1, -1, -1, -1, -1) {}

    /// A time given its 6 components
    FuzzyTime(int ye, int mo = -1, int da = -1, int ho = -1, int mi = -1,
              int se = -1)
        : TimeBase(ye, mo, da, ho, mi, se)
    {
    }

    /// Return a Time object with the earlier possible time
    Time lowerbound();

    /// Return a Time object with the latest possible time
    Time upperbound();

    /// Return the time formatted as a string in SQL-like format
    std::string to_string() const;

    /// Set to easter day, with ho, mi, and se, set to -1
    void set_easter(int year);

    /// Raise std::invalid_argument if the set portion of the date is not well
    /// formed
    void validate() const;
};

} // namespace core
} // namespace arki

#endif
