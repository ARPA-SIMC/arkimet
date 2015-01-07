#ifndef ARKI_TYPES_BBOX_H
#define ARKI_TYPES_BBOX_H

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

/**
 * WARNING
 * This metadata type is discontinued, and it exists only to preserve
 * compatibility with existing saved data
 */


#include <arki/types.h>

struct lua_State;

namespace arki {
namespace types {

struct BBox;

template<>
struct traits<BBox>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The bbox of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct BBox : public types::StyledType<BBox>
{
	/// Style values
	//static const Style NONE = 0;
	static const Style INVALID = 1;
	static const Style POINT = 2;
	static const Style BOX = 3;
	static const Style HULL = 4;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::auto_ptr<BBox> decode(const unsigned char* buf, size_t len);
    static std::auto_ptr<BBox> decodeString(const std::string& val);
    static std::auto_ptr<BBox> decodeMapping(const emitter::memory::Mapping& val);

    // Register this type tree with the type system
    static void init();

    static std::auto_ptr<BBox> createInvalid();
};

namespace bbox {

struct INVALID : public BBox
{
    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    const char* lua_type_name() const override;

    int compare_local(const BBox& o) const override;
    bool equals(const Type& o) const override;

    INVALID* clone() const override;
    static std::auto_ptr<INVALID> create();
};

}

}
}

// vim:set ts=4 sw=4:
#endif
