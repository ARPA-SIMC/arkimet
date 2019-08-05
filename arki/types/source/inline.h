#ifndef ARKI_TYPES_SOURCE_INLINE_H
#define ARKI_TYPES_SOURCE_INLINE_H

#include <arki/types/source.h>

namespace arki {
namespace types {
namespace source {

struct Inline : public Source
{
    uint64_t size;

    Style style() const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;

    Inline* clone() const override;

    static std::unique_ptr<Inline> create(const std::string& format, uint64_t size);
    static std::unique_ptr<Inline> decodeMapping(const emitter::memory::Mapping& val);
};


}
}
}
#endif
