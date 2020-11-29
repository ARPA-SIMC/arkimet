#ifndef ARKI_TYPES_STYLED_TCC
#define ARKI_TYPES_STYLED_TCC

#include "arki/types/styled.h"
#include "arki/core/binary.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>

namespace arki {
namespace types {

template<typename BASE>
void StyledType<BASE>::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_unsigned((unsigned)this->style(), 1);
}

template<typename BASE>
int StyledType<BASE>::compare(const Type& o) const
{
    int res = CoreType<BASE>::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const BASE* v = dynamic_cast<const BASE*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `" << traits<BASE>::type_tag << "', but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    return this->compare_local(*v);
}

template<typename BASE>
void StyledType<BASE>::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, BASE::formatStyle(style()));
}

template<typename BASE>
typename StyledType<BASE>::Style StyledType<BASE>::style_from_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    return BASE::parseStyle(reader.as_string(keys.type_style, "type style"));
}

}
}

#include "arki/types/core.tcc"

#endif
