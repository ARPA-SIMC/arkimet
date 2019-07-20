#ifndef ARKI_DATASET_TIME_H
#define ARKI_DATASET_TIME_H

#include <arki/core/time.h>
#include <arki/dataset/fwd.h>
#include <ctime>

namespace arki {
namespace dataset {

struct SessionTimeOverride
{
    SessionTime* orig = nullptr;

    SessionTimeOverride() = default;
    SessionTimeOverride(SessionTime* orig);
    SessionTimeOverride(const SessionTimeOverride&) = delete;
    SessionTimeOverride(SessionTimeOverride&&);
    SessionTimeOverride& operator=(const SessionTimeOverride&) = delete;
    SessionTimeOverride& operator=(SessionTimeOverride&&);
    ~SessionTimeOverride();
};


struct SessionTime
{
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
    static SessionTimeOverride local_override(time_t new_value);
};

}
}

#endif
