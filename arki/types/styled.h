#ifndef ARKI_TYPES_STYLED_H
#define ARKI_TYPES_STYLED_H

#include <arki/types/core.h>
#include <arki/core/fwd.h>

namespace arki {
namespace types {

template<typename BASE>
struct StyledType : public CoreType<BASE>
{
    typedef typename traits<BASE>::Style Style;

    // Get the element style
    virtual Style style() const = 0;

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

    virtual void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const;

    static Style style_from_structure(const structured::Keys& keys, const structured::Reader& reader);
};

}
}

#endif
