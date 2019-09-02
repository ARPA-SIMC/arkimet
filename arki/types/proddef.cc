#include "arki/exceptions.h"
#include "arki/types/proddef.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#define CODE TYPE_PRODDEF
#define TAG "proddef"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Proddef>::type_tag = TAG;
const types::Code traits<Proddef>::type_code = CODE;
const size_t traits<Proddef>::type_sersize_bytes = SERSIZELEN;

Proddef::Style Proddef::parseStyle(const std::string& str)
{
    if (str == "GRIB") return Style::GRIB;
    throw_consistency_error("parsing Proddef style", "cannot parse Proddef style '"+str+"': only GRIB is supported");
}

std::string Proddef::formatStyle(Proddef::Style s)
{
    switch (s)
    {
        case Style::GRIB: return "GRIB";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

unique_ptr<Proddef> Proddef::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "proddef style");
    switch (s)
    {
        case Style::GRIB:
            return createGRIB(ValueBag::decode(dec));
        default:
            throw_consistency_error("parsing Proddef", "style is " + formatStyle(s) + " but we can only decode GRIB");
    }
}

unique_ptr<Proddef> Proddef::decodeString(const std::string& val)
{
    string inner;
    Proddef::Style style = outerParse<Proddef>(val, inner);
    switch (style)
    {
        case Style::GRIB: return createGRIB(ValueBag::parse(inner));
        default:
            throw_consistency_error("parsing Proddef", "unknown Proddef style " + formatStyle(style));
    }
}

std::unique_ptr<Proddef> Proddef::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::GRIB: return upcast<Proddef>(proddef::GRIB::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown Proddef style");
    }
}

unique_ptr<Proddef> Proddef::createGRIB(const ValueBag& values)
{
    return upcast<Proddef>(proddef::GRIB::create(values));
}

namespace proddef {

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

Proddef::Style GRIB::style() const { return Style::GRIB; }

void GRIB::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Proddef::encodeWithoutEnvelope(enc);
    m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void GRIB::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Proddef::serialise_local(e, keys, f);
    e.add(keys.proddef_value);
    m_values.serialise(e);
}

std::unique_ptr<GRIB> GRIB::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    std::unique_ptr<GRIB> res;
    val.sub(keys.proddef_value, "proddef value", [&](const structured::Reader& values) {
        res = GRIB::create(ValueBag::parse(values));
    });
    return res;
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}

int GRIB::compare_local(const Proddef& o) const
{
    if (int res = Proddef::compare_local(o)) return res;

	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB Proddef, but is a ") + typeid(&o).name() + " instead");

	return m_values.compare(v->m_values);
}

bool GRIB::equals(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

GRIB* GRIB::clone() const
{
    GRIB* res = new GRIB;
    res->m_values = m_values;
    return res;
}

unique_ptr<GRIB> GRIB::create(const ValueBag& values)
{
    GRIB* res = new GRIB;
    res->m_values = values;
    return unique_ptr<GRIB>(res);
}

}

void Proddef::init()
{
    MetadataType::register_type<Proddef>();
}

}
}
#include <arki/types/styled.tcc>
