#ifndef ARKI_EMITTER_KEYS_H
#define ARKI_EMITTER_KEYS_H

namespace arki {
namespace emitter {

struct Keys
{
    const char* type_name;
    const char* type_desc;
};

extern const Keys keys_json;
extern const Keys keys_python;

}
}

#endif
