#include "arki/types/field.h"
#include "arki/core/binary.h"
#include "arki/exceptions.h"
#include "arki/stream/text.h"
#include "arki/structured/emitter.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
#include "arki/types/utils.h"
#include <sstream>

#define CODE TYPE_FIELD
#define TAG "field"
#define SERSIZELEN 1

using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Field>::type_tag            = TAG;
const types::Code traits<Field>::type_code     = CODE;
const size_t traits<Field>::type_sersize_bytes = SERSIZELEN;

std::string Field::get() const
{
    core::BinaryDecoder dec(data, size);
    size_t vallen = dec.pop_varint<size_t>("field text size");
    return dec.pop_string(vallen, "field text");
}

int Field::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0)
        return res;

    // We should be the same kind, so upcast
    const Field* v = dynamic_cast<const Field*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            std::string("second element claims to be a Field, but it is a ") +
                typeid(&o).name() + " instead");

    return get().compare(v->get());
}

std::unique_ptr<Field> Field::decode(core::BinaryDecoder& dec,
                                     bool reuse_buffer)
{
    dec.ensure_size(1, "Field data");
    std::unique_ptr<Field> res;
    if (reuse_buffer)
        res.reset(new Field(dec.buf, dec.size, false));
    else
        res.reset(new Field(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::ostream& Field::writeToOstream(std::ostream& o) const
{
    return o << get();
}

void Field::serialise_local(structured::Emitter& e,
                            const structured::Keys& keys,
                            const Formatter* f) const
{
    e.add(keys.field_value, get());
}

std::unique_ptr<Field> Field::decode_structure(const structured::Keys& keys,
                                               const structured::Reader& val)
{
    return Field::create(val.as_string(keys.field_value, "Field value"));
}

std::unique_ptr<Field> Field::decodeString(const std::string& val)
{
    if (val.empty())
        throw_consistency_error("parsing Field", "string is empty");
    return Field::create(val);
}

Field* Field::clone() const { return new Field(data, size); }

std::unique_ptr<Field> Field::create(const std::string& val)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_varint(val.size());
    enc.add_raw(val);
    return std::unique_ptr<Field>(new Field(buf));
}

void Field::write_documentation(stream::Text& out, unsigned heading_level)
{
    out.rst_header("Field", heading_level);

    out.print(R"(
Application field.

If set, this identifies an application field that can provide an extra set of
assumptions about the contents of the data.

This can be used, for example, to infer assumptions about the internal layout
of a NetCDF or HDF5 data file.
)");
}

void Field::init() { MetadataType::register_type<Field>(); }

} // namespace types
} // namespace arki
