#ifndef ARKI_DATASET_TIME_H
#define ARKI_DATASET_TIME_H

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

    static const SessionTime& get();

    static Override local_override(time_t new_value);
};

}
}

#endif
