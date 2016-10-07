#include "time.h"

using namespace std;

namespace arki {
namespace dataset {

namespace {

static SessionTime* current_session_time = nullptr;


struct CurrentTime : public SessionTime
{
    time_t now() const override
    {
        return time(nullptr);
    }
};

struct FixedTime : public SessionTime
{
    time_t val;

    FixedTime(time_t val) : val(val) {}

    time_t now() const override
    {
        return val;
    }
};

}

SessionTime::Override::Override(SessionTime* orig)
    : orig(orig) {}

SessionTime::Override::Override(Override&& o)
    : orig(o.orig)
{
    o.orig = nullptr;
}

SessionTime::Override& SessionTime::Override::operator=(Override&& o)
{
    if (&o == this) return *this;
    orig = o.orig;
    o.orig = nullptr;
    return *this;
}

SessionTime::Override::~Override()
{
    if (orig != nullptr)
    {
        delete current_session_time;
        current_session_time = orig;
    }
}

const SessionTime& SessionTime::get()
{
    if (!current_session_time)
        current_session_time = new CurrentTime;

    return *current_session_time;
}

SessionTime::Override SessionTime::local_override(time_t new_value)
{
    if (!current_session_time)
        current_session_time = new CurrentTime;

    SessionTime::Override res(current_session_time);
    current_session_time = new FixedTime(new_value);
    return res;
}

}
}
