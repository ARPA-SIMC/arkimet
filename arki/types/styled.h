#ifndef ARKI_TYPES_STYLED_H
#define ARKI_TYPES_STYLED_H

#include <arki/types/core.h>

namespace arki {
namespace types {

template<typename BASE>
struct StyledType : public CoreType<BASE>
{
    typedef typename traits<BASE>::Style Style;

    // Get the element style
    virtual Style style() const = 0;

    // Default implementations of Type methods
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    int compare(const Type& o) const override;
    virtual int compare_local(const BASE& o) const
    {
        if (this->style() < o.style())
            return -1;
        if (this->style() > o.style())
            return 1;
        return 0;
    }

    virtual void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const;

    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    static Style style_from_mapping(const emitter::memory::Mapping& m);
    static Style style_from_structure(const emitter::Keys& keys, const emitter::Reader& reader);
};

}
}

#endif
