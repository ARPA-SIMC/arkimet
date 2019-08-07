#include "emitter.h"
#include "arki/types.h"
#include "arki/core/time.h"

namespace arki {
namespace structured {

void Emitter::add_break() {}
void Emitter::add_raw(const std::string& val) {}
void Emitter::add_raw(const std::vector<uint8_t>& val) {}
void Emitter::add_type(const types::Type& t, const structured::Keys& keys, const Formatter* f) { t.serialise(*this, keys, f); }
void Emitter::add_time(const core::Time& val)
{
    start_list();
    add(val.ye);
    add(val.mo);
    add(val.da);
    add(val.ho);
    add(val.mi);
    add(val.se);
    end_list();
}

}
}
