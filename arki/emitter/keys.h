#ifndef ARKI_EMITTER_KEYS_H
#define ARKI_EMITTER_KEYS_H

namespace arki {
namespace emitter {

struct Keys
{
    const char* type_name;
    const char* type_desc;
    const char* type_style;
    const char* reftime_position_time;
    const char* reftime_period_begin;
    const char* reftime_period_end;
};

extern const Keys keys_json;
extern const Keys keys_python;

}
}

#endif
