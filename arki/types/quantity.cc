#include "arki/exceptions.h"
#include "arki/types/quantity.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#define CODE TYPE_QUANTITY
#define TAG "quantity"
#define SERSIZELEN 	1

using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Quantity>::type_tag = TAG;
const types::Code traits<Quantity>::type_code = CODE;
const size_t traits<Quantity>::type_sersize_bytes = SERSIZELEN;

std::set<std::string> Quantity::get() const
{
    core::BinaryDecoder dec(data, size);
    size_t num = dec.pop_varint<size_t>("quantity num elements");
    std::set<std::string> vals;

    for (size_t i = 0; i < num; ++i)
    {
        size_t vallen = dec.pop_varint<size_t>("quantity name len");
        std::string val = dec.pop_string(vallen, "quantity name");
        vals.insert(val);
    }

    return vals;
}

int Quantity::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

    // We should be the same kind, so upcast
    const Quantity* v = dynamic_cast<const Quantity*>(&o);
    if (!v)
        throw_consistency_error(
                "comparing metadata types",
                std::string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

    // TODO: we can probably do better than this
    std::ostringstream ss1;
    std::ostringstream ss2;

    writeToOstream(ss1);
    v->writeToOstream(ss2);

    return ss1.str().compare(ss2.str());
}

std::unique_ptr<Quantity> Quantity::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Quantity data");
    return std::unique_ptr<Quantity>(new Quantity(dec.buf, dec.size));
}

std::ostream& Quantity::writeToOstream(std::ostream& o) const
{
    auto values = get();
    return o << str::join(", ", values.begin(), values.end());
}

void Quantity::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto values = get();
    e.add(keys.quantity_value);
    e.start_list();
    for (const auto& value: values)
        e.add(value);
    e.end_list();
}

std::unique_ptr<Quantity> Quantity::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    std::set<std::string> vals;
    val.sub(keys.quantity_value, "Quantity values", [&](const structured::Reader& list) {
        unsigned size = list.list_size("Quantity values");
        for (unsigned i = 0; i < size; ++i)
            vals.insert(list.as_string(i, "quantity value"));
    });
    return Quantity::create(vals);
}

std::unique_ptr<Quantity> Quantity::decodeString(const std::string& val)
{
	if (val.empty())
		throw_consistency_error("parsing Quantity", "string is empty");

	std::set<std::string> vals;
	split(val, vals);
	return Quantity::create(vals);
}

Quantity* Quantity::clone() const
{
    return new Quantity(data, size);
}

std::unique_ptr<Quantity> Quantity::create(const std::string& values)
{
    std::set<std::string> vals;
    split(values, vals);
    return Quantity::create(vals);
}

std::unique_ptr<Quantity> Quantity::create(const std::set<std::string>& values)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_varint(values.size());
    for (const auto& v: values)
    {
        enc.add_varint(v.size());
        enc.add_raw(v);
    }
    return std::unique_ptr<Quantity>(new Quantity(buf));
}


void Quantity::init()
{
    MetadataType::register_type<Quantity>();
}

}
}
