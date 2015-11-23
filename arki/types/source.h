#ifndef ARKI_TYPES_SOURCE_H
#define ARKI_TYPES_SOURCE_H

/*
 * types/source - Source information
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

#include <arki/libconfig.h>
#include <arki/types.h>
#include <wibble/sys/buffer.h>
#include <stddef.h>
#include <stdint.h>

struct lua_State;

namespace arki {
namespace types {

struct Source;

template<>
struct traits<Source>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The place where the data is stored
 */
struct Source : public types::StyledType<Source>
{
    std::string format;

	/// Style values
	//static const Style NONE = 0;
	static const Style BLOB = 1;
	static const Style URL = 2;
	static const Style INLINE = 3;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	virtual int compare_local(const Source& o) const;

    /// CODEC functions
    virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
    static std::unique_ptr<Source> decode(const unsigned char* buf, size_t len);
    static std::unique_ptr<Source> decodeRelative(const unsigned char* buf, size_t len, const std::string& basedir);
    static std::unique_ptr<Source> decodeString(const std::string& val);
    static std::unique_ptr<Source> decodeMapping(const emitter::memory::Mapping& val);
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;

    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    Source* clone() const = 0;

    static std::unique_ptr<Source> createBlob(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static std::unique_ptr<Source> createInline(const std::string& format, uint64_t size);
    static std::unique_ptr<Source> createURL(const std::string& format, const std::string& url);
};

}
}

// vim:set ts=4 sw=4:
#endif
