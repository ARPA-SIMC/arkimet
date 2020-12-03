#ifndef ARKI_TYPES_ENCODED_H
#define ARKI_TYPES_ENCODED_H

#include <arki/types.h>
#include <arki/core/fwd.h>

namespace arki {
namespace types {

class Encoded : public Type
{
protected:
    const uint8_t* data = nullptr;
    unsigned size = 0;
    bool owned = true;

public:
    /// Construct copying a vector contents
    Encoded(const std::vector<uint8_t>& buf);

    /// Construct copying a buffer
    Encoded(const uint8_t* buf, unsigned size);

    /**
     * Construct acquiring a pointer.
     *
     * No copy will be made, the pointer will be used as is.
     *
     * If owned is true, the buffer will be managed by Encoded and deallocated
     * on destruction. If false, the buffer is supposed to remain allocated
     * during the whole lifetime of the Encoded object
     */
    Encoded(const uint8_t* buf, unsigned size, bool owned);

    Encoded(const Encoded&) = delete;

    Encoded(Encoded&& o)
        : data(o.data), size(o.size)
    {
        o.data = nullptr;
        o.size = 0;
    }

    ~Encoded();

    Encoded& operator=(const Encoded&) = delete;
    Encoded& operator=(Encoded&&) = delete;

    bool equals(const Type& o) const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
};

}
}

#endif

