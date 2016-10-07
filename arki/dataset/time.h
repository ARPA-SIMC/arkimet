#ifndef ARKI_DATASET_TIME_H
#define ARKI_DATASET_TIME_H

#include <arki/core/time.h>
#include <ctime>

namespace arki {
namespace dataset {

struct SessionTime
{
    struct Override
    {
        SessionTime* orig;

        Override(SessionTime* orig);
        Override(const Override&) = delete;
        Override(Override&&);
        Override& operator=(const Override&) = delete;
        Override& operator=(Override&&);
        ~Override();
    };

    virtual ~SessionTime() {}

    /// Return the current time
    virtual time_t now() const = 0;

    /**
     * Return the time of the midnight at the beginning of the day `age` days
     * before today.
     */
    arki::core::Time age_threshold(unsigned age) const;

    /**
     * Get the current SessionTime object.
     *
     * It normally returns a SessionTime that works using the current time, but
     * can be overridden for testing.
     */
    static const SessionTime& get();

    /**
     * Override the current time with the given time, and restore the previous
     * value when the returned Override object goes out of scope.
     */
    static Override local_override(time_t new_value);
};

}
}

#endif
