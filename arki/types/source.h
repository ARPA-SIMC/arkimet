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
#include <wibble/sys/buffer.h>
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
protected:
    /**
     * Inline data, or cached version of previously read data
     */
    mutable wibble::sys::Buffer m_inline_buf;

public:
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
    static Item<Source> decode(const unsigned char* buf, size_t len);
    static Item<Source> decodeRelative(const unsigned char* buf, size_t len, const std::string& basedir);
    static Item<Source> decodeString(const std::string& val);
    static Item<Source> decodeMapping(const emitter::memory::Mapping& val);
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;

    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    // Management functions for inline or cached data

    /**
     * Returns true if data is available without having to load it (either
     * inline or cached)
     */
    bool hasData() const;

    /**
     * If the source is not inline, but the data are cached in memory, drop
     * them.
     *
     * Data for non-inline metadata can be cached in memory, for example,
     * by a getData() call or a setCachedData() call.
     */
    virtual void dropCachedData() const;

    /**
     * Read the raw blob data described by this metadata.
     *
     * Optionally, a directory can be given as a base to resolve relative
     * paths.
     */
    virtual wibble::sys::Buffer getData() const = 0;

    /**
     * Set cached data for non-inline sources, so that getData() won't have
     * to read it again.
     */
    void setCachedData(const wibble::sys::Buffer& buf) const;

    /**
     * Flush open data readers.
     *
     * A persistent data reader is used to read data, in order to keep the last
     * file opened and buffered to speed up reading multiple data items from
     * the same file. This function tells the data reader to close its open
     * files.
     *
     * It is useful for testing cases when data files are moved or
     * compressed.
     */
    static void flushDataReaders();
};

namespace source {

struct Blob : public Source
{
    /**
     * Base directory used to resolve relative filenames.
     *
     * Note that this is not stored when serializing, since metadata usually
     * point to files relative to the metadata location, in order to save
     * space.
     */
    std::string basedir;

    /**
     * Data file name.
     *
     * Can be an absolute or a relative path. If it is a relative path, it is
     * resolved based on \a basedir, or on the current directory if \a basedir
     * is empty.
     */
    std::string filename;

    uint64_t offset;
    uint64_t size;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Source& o) const;
	virtual bool operator==(const Type& o) const;

    /// Return the absolute pathname to the data file
    std::string absolutePathname() const;

    /**
     * Return a new source identical to this one, but with all the
     * directory components stripped from the file name.
     *
     * basedir is updated so that we can still reach the data file.
     */
    Item<Blob> fileOnly() const;

    virtual wibble::sys::Buffer getData() const;

    static Item<Blob> create(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static Item<Blob> decodeMapping(const emitter::memory::Mapping& val);
};

struct URL : public Source
{
	std::string url;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Source& o) const;
	virtual bool operator==(const Type& o) const;

    virtual wibble::sys::Buffer getData() const;

	static Item<URL> create(const std::string& format, const std::string& url);
	static Item<URL> decodeMapping(const emitter::memory::Mapping& val);
};

struct Inline : public Source
{
	uint64_t size;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Source& o) const;
	virtual bool operator==(const Type& o) const;

    virtual void dropCachedData() const;
    virtual wibble::sys::Buffer getData() const;

	static Item<Inline> create(const std::string& format, uint64_t size);
	static Item<Inline> decodeMapping(const emitter::memory::Mapping& val);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
