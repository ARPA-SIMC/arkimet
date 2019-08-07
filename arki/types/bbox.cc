#include "arki/exceptions.h"
#include "arki/types/bbox.h"
#include "arki/types/utils.h"
#include "arki/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/libconfig.h"
#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif
#include <iomanip>
#include <sstream>

#define CODE TYPE_BBOX
#define TAG "bbox"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<BBox>::type_tag = TAG;
const types::Code traits<BBox>::type_code = CODE;
const size_t traits<BBox>::type_sersize_bytes = SERSIZELEN;
const char* traits<BBox>::type_lua_tag = LUATAG_TYPES ".bbox";

BBox::Style BBox::parseStyle(const std::string& str)
{
    if (str == "INVALID") return Style::INVALID;
    if (str == "POINT") return Style::POINT;
    if (str == "BOX") return Style::BOX;
    if (str == "HULL") return Style::HULL;
    throw_consistency_error("parsing BBox style", "cannot parse BBox style '"+str+"': only INVALID and BOX are supported");
}

std::string BBox::formatStyle(BBox::Style s)
{
    switch (s)
    {
        //case BBox::NONE: return "NONE";
        case Style::INVALID: return "INVALID";
        case Style::POINT: return "POINT";
        case Style::BOX: return "BOX";
        case Style::HULL: return "HULL";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

unique_ptr<BBox> BBox::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "bbox style");
    switch (s)
    {
        case Style::INVALID:
            return createInvalid();
        case Style::POINT:
            for (int i = 0; i < 2; ++i)
                dec.pop_float("old POINT bbox value");
            return createInvalid();
        case Style::BOX:
            for (int i = 0; i < 4; ++i)
                dec.pop_float("old BOX bbox value");
            return createInvalid();
        case Style::HULL: {
            size_t pointCount = dec.pop_uint(2, "HULL bbox vertex count");
            for (size_t i = 0; i < pointCount * 2; ++i)
                dec.pop_float("old HULL bbox vertex data");
            return createInvalid();
        }
        default:
            throw_consistency_error("parsing BBox", "style is " + formatStyle(s) + " but we can only decode INVALID and BOX");
    }
}

unique_ptr<BBox> BBox::decodeString(const std::string& val)
{
    return createInvalid();
}

std::unique_ptr<BBox> BBox::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return createInvalid();
}

unique_ptr<BBox> BBox::createInvalid()
{
    return upcast<BBox>(bbox::INVALID::create());
}


namespace bbox {

BBox::Style INVALID::style() const { return Style::INVALID; }

void INVALID::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    BBox::encodeWithoutEnvelope(enc);
}
std::ostream& INVALID::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "()";
}
const char* INVALID::lua_type_name() const { return "arki.types.bbox.invalid"; }

int INVALID::compare_local(const BBox& o) const
{
    if (int res = BBox::compare_local(o)) return res;

	// We should be the same kind, so upcast
	const INVALID* v = dynamic_cast<const INVALID*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 BBox, but is a ") + typeid(&o).name() + " instead");

	return 0;
}

bool INVALID::equals(const Type& o) const
{
	const INVALID* v = dynamic_cast<const INVALID*>(&o);
	if (!v) return false;
	return true;
}

INVALID* INVALID::clone() const
{
    return new INVALID;
}

unique_ptr<INVALID> INVALID::create()
{
    return unique_ptr<INVALID>(new INVALID);
}

}

void BBox::init()
{
    MetadataType::register_type<BBox>();
}

}
}

#include <arki/types/styled.tcc>
