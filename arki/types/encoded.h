#ifndef ARKI_TYPES_ENCODED_H
#define ARKI_TYPES_ENCODED_H

#include <arki/types/core.h>
#include <arki/core/fwd.h>

namespace arki {
namespace types {

class Encoded : public Type
{
protected:
    std::vector<uint8_t> data;

public:
    Encoded(const std::vector<uint8_t>& data);
    Encoded(std::vector<uint8_t>&& data);
    ~Encoded();

#if 0
    // Default implementations of Type methods
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    int compare(const Type& o) const override;
    virtual int compare_local(const BASE& o) const
    {
        if (this->style() < o.style())
            return -1;
        if (this->style() > o.style())
            return 1;
        return 0;
    }

    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
#endif
};

}
}

#endif

