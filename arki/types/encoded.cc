#include "encoded.h"
#include "arki/core/binary.h"
#include <cstring>

namespace arki {
namespace types {

Encoded::Encoded(const std::vector<uint8_t>& buf)
    : size(buf.size())
{
    data = new uint8_t[size];
    memcpy(data, buf.data(), size);
}

Encoded::Encoded(const uint8_t* buf, unsigned size)
    : size(size)
{
    data = new uint8_t[size];
    memcpy(data, buf, size);
}

Encoded::Encoded(uint8_t*&& buf, unsigned&& size)
    : data(buf), size(size)
{
    buf = nullptr;
    size = 0;
}

Encoded::~Encoded()
{
    delete[] data;
}


bool Encoded::equals(const Type& o) const
{
    if (type_code() != o.type_code()) return false;
    // This can be a reinterpret_cast for performance, since we just validated
    // the type code
    const Encoded* v = reinterpret_cast<const Encoded*>(&o);
    if (!v) return false;
    if (size != v->size) return false;
    return memcmp(data, v->data, size) == 0;
}

void Encoded::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_raw(data, size);
}


}
}
