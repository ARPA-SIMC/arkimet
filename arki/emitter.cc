#include <arki/emitter.h>
#include <arki/types.h>

namespace arki {

void Emitter::add_break() {}
void Emitter::add_raw(const std::string& val) {}
void Emitter::add_raw(const std::vector<uint8_t>& val) {}
void Emitter::add_type(const types::Type& t, const Formatter* f) { t.serialise(*this, f); }

}
