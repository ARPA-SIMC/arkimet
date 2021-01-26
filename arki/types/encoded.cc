#include "encoded.h"
#include "arki/core/binary.h"
#include <cstring>

namespace arki {
namespace types {

Encoded::Encoded(const std::vector<uint8_t>& buf)
    : size(buf.size())
{
    uint8_t* tdata = new uint8_t[size];
    memcpy(tdata, buf.data(), size);
    data = tdata;
}

Encoded::Encoded(const uint8_t* buf, unsigned size, bool owned)
    : data(buf), size(size), owned(owned)
{
}

Encoded::Encoded(const uint8_t* buf, unsigned size)
    : size(size)
{
    uint8_t* tdata = new uint8_t[size];
    memcpy(tdata, buf, size);
    data = tdata;
}

Encoded::~Encoded()
{
    if (owned)
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
