#include "inline.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include "arki/exceptions.h"
#include <ostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {
namespace source {

source::Style Inline::style() const { return source::Style::INLINE; }

void Inline::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.add_varint(size);
}

std::ostream& Inline::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
             << format << "," << size
             << ")";
}
void Inline::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Source::serialise_local(e, keys, f);
    e.add(keys.source_size, size);
}

std::unique_ptr<Inline> Inline::decode_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    return Inline::create(
            reader.as_string(keys.source_format, "source format"),
            reader.as_int(keys.source_size, "source size"));
}

int Inline::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const Inline* v = dynamic_cast<const Inline*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a Inline Source, but is a ") + typeid(&o).name() + " instead");

    return size - v->size;
}
bool Inline::equals(const Type& o) const
{
    const Inline* v = dynamic_cast<const Inline*>(&o);
    if (!v) return false;
    return format == v->format && size == v->size;
}

Inline* Inline::clone() const
{
    return new Inline(*this);
}

std::unique_ptr<Inline> Inline::create(const std::string& format, uint64_t size)
{
    Inline* res = new Inline;
    res->format = format;
    res->size = size;
    return unique_ptr<Inline>(res);
}

}
}
}
