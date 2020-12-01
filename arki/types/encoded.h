#ifndef ARKI_TYPES_ENCODED_H
#define ARKI_TYPES_ENCODED_H

#include <arki/types/core.h>
#include <arki/core/fwd.h>

namespace arki {
namespace types {

class Encoded : public Type
{
protected:
    uint8_t* data = nullptr;
    unsigned size = 0;

public:
    /// Construct copying a vector contents
    Encoded(const std::vector<uint8_t>& buf);

    /// Construct copying a buffer
    Encoded(const uint8_t* buf, unsigned size);

    /**
     * Construct stealing a buffer pointer
     *
     * buf will be memory managed by the Encoded object
     */
    Encoded(uint8_t*&& buf, unsigned&& size);

    Encoded(const Encoded&) = delete;
    Encoded(Encoded&& o) = delete;
    ~Encoded();

    Encoded& operator=(const Encoded&) = delete;
    Encoded& operator=(Encoded&&) = delete;

    bool equals(const Type& o) const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
};

}
}

#endif

