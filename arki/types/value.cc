#include "arki/types/value.h"
#include "arki/core/binary.h"
#include "arki/stream/text.h"
#include "arki/structured/emitter.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
#include "arki/types/utils.h"
#include "arki/utils/string.h"
#include <sstream>

#define CODE TYPE_VALUE
#define TAG "value"
#define SERSIZELEN 0 // Not supported in version 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Value>::type_tag            = TAG;
const types::Code traits<Value>::type_code     = CODE;
const size_t traits<Value>::type_sersize_bytes = SERSIZELEN;

bool Value::equals(const Type& o) const
{
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v)
        return false;
    return buffer == v->buffer;
}

int Value::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0)
        return res;

    // We should be the same kind, so upcast
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v)
    {
        stringstream ss;
        ss << "cannot compare metadata type: second element claims to be "
              "`value', but is `"
           << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    // Just compare buffers
    if (buffer < v->buffer)
        return -1;
    if (buffer == v->buffer)
        return 0;
    return 1;
}

void Value::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_raw(buffer);
}

std::ostream& Value::writeToOstream(std::ostream& o) const
{
    return o << str::encode_cstring(buffer);
}

void Value::serialise_local(structured::Emitter& e,
                            const structured::Keys& keys,
                            const Formatter* f) const
{
    e.add(keys.value_value, buffer);
}

unique_ptr<Value> Value::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    return Value::create(dec.pop_string(dec.size, "'value' metadata type"));
}

unique_ptr<Value> Value::decodeString(const std::string& val)
{
    size_t len;
    return Value::create(str::decode_cstring(val, len));
}

std::unique_ptr<Value> Value::decode_structure(const structured::Keys& keys,
                                               const structured::Reader& val)
{
    return Value::create(
        val.as_string(keys.value_value, "item value encoded in metadata"));
}

Value* Value::clone() const
{
    Value* val  = new Value;
    val->buffer = buffer;
    return val;
}

unique_ptr<Value> Value::create(const std::string& buf)
{
    Value* val  = new Value;
    val->buffer = buf;
    return unique_ptr<Value>(val);
}

void Value::write_documentation(stream::Text& out, unsigned heading_level)
{
    out.rst_header("Value", heading_level);
    out.print(Value::doc);
}

void Value::init() { MetadataType::register_type<Value>(); }

} // namespace types
} // namespace arki
