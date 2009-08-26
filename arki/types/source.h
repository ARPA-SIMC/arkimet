#ifndef ARKI_TYPES_SOURCE_H
#define ARKI_TYPES_SOURCE_H

/*
 * types/source - Source information
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types.h>

struct lua_State;

namespace arki {
namespace types {

/**
 * The place where the data is stored
 */
struct Source : public types::Type
{
	typedef unsigned char Style;
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
	/// Source style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Source& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<Source> decode(const unsigned char* buf, size_t len);
	static Item<Source> decodeString(const std::string& val);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Source
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Source LUA object
	static int lua_lookup(lua_State* L);
};

namespace source {

struct Blob : public Source
{
	std::string filename;
	size_t offset;
	size_t size;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Source& o) const;
	virtual int compare(const Blob& o) const;
	virtual bool operator==(const Type& o) const;

	/**
	 * Return a new source identical to this one, but with the given path prepended to the file name.
	 */
	Item<Blob> prependPath(const std::string& path);

	static Item<Blob> create(const std::string& format, const std::string& filename, size_t offset, size_t size);
};

struct URL : public Source
{
	std::string url;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Source& o) const;
	virtual int compare(const URL& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<URL> create(const std::string& format, const std::string& url);
};

struct Inline : public Source
{
	size_t size;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Source& o) const;
	virtual int compare(const Inline& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<Inline> create(const std::string& format, size_t size);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
