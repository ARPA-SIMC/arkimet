#ifndef ARKI_TYPES_SOURCE_INLINE_H
#define ARKI_TYPES_SOURCE_INLINE_H

#include <arki/types/source.h>

namespace arki {
namespace types {
namespace source {

class Inline : public Source
{
public:
    uint64_t size;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;

    Inline* clone() const override;

    static std::unique_ptr<Inline> create(const std::string& format, uint64_t size);
    static std::unique_ptr<Inline> decode_structure(const structured::Keys& keys, const structured::Reader& reader);
};


}
}
}
#endif
