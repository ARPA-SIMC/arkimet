/*
 * types/bbox - Bounding box metadata item
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <arki/types/bbox.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#ifdef HAVE_GEOS
#include <memory>
#if GEOS_VERSION < 3
#include <geos/geom.h>

using namespace geos;

typedef DefaultCoordinateSequence CoordinateArraySequence;
#else
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>

using namespace geos::geom;
#endif
#endif

#define CODE types::TYPE_BBOX
#define TAG "bbox"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

const char* traits<BBox>::type_tag = TAG;
const types::Code traits<BBox>::type_code = CODE;
const size_t traits<BBox>::type_sersize_bytes = SERSIZELEN;
const char* traits<BBox>::type_lua_tag = LUATAG_TYPES ".bbox";

// Style constants
//const unsigned char BBox::NONE;
const unsigned char BBox::INVALID;
const unsigned char BBox::POINT;
const unsigned char BBox::BOX;
const unsigned char BBox::HULL;

BBox::Style BBox::parseStyle(const std::string& str)
{
	if (str == "INVALID") return INVALID;
	if (str == "POINT") return POINT;
	if (str == "BOX") return BOX;
	if (str == "HULL") return HULL;
	throw wibble::exception::Consistency("parsing BBox style", "cannot parse BBox style '"+str+"': only INVALID and BOX are supported");
}

std::string BBox::formatStyle(BBox::Style s)
{
	switch (s)
	{
		//case BBox::NONE: return "NONE";
		case BBox::INVALID: return "INVALID";
		case BBox::POINT: return "POINT";
		case BBox::BOX: return "BOX";
		case BBox::HULL: return "HULL";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<BBox> BBox::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "BBox");
	Style s = (Style)decodeUInt(buf, 1);
    switch (s)
    {
        case INVALID:
            return createInvalid();
        case POINT:
            ensureSize(len, 9, "BBox");
            decodeFloat(buf+1), decodeFloat(buf+5);
            return createInvalid();
        case BOX:
            ensureSize(len, 17, "BBox");
            decodeFloat(buf+1), decodeFloat(buf+5), decodeFloat(buf+9), decodeFloat(buf+13);
            return createInvalid();
        case HULL: {
            ensureSize(len, 3, "BBox");
            size_t pointCount = decodeUInt(buf+1, 2);
            ensureSize(len, 3+pointCount*8, "BBox");
            for (size_t i = 0; i < pointCount; ++i)
                decodeFloat(buf+3+i*8), decodeFloat(buf+3+i*8+4);
            return createInvalid();
        }
        default:
            throw wibble::exception::Consistency("parsing BBox", "style is " + formatStyle(s) + " but we can only decode INVALID and BOX");
    }
}

unique_ptr<BBox> BBox::decodeString(const std::string& val)
{
    return createInvalid();
}

unique_ptr<BBox> BBox::decodeMapping(const emitter::memory::Mapping& val)
{
    return createInvalid();
}

unique_ptr<BBox> BBox::createInvalid()
{
    return upcast<BBox>(bbox::INVALID::create());
}


namespace bbox {

BBox::Style INVALID::style() const { return BBox::INVALID; }

void INVALID::encodeWithoutEnvelope(Encoder& enc) const
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
	// We should be the same kind, so upcast
	const INVALID* v = dynamic_cast<const INVALID*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
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
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
