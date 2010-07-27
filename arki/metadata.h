#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/*
 * metadata - Handle xgribarch metadata
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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


#include <arki/itemset.h>
#include <arki/types.h>
#include <arki/types/note.h>
#include <arki/types/source.h>
#include <wibble/sys/buffer.h>
#include <string>

struct lua_State;

namespace arki {

namespace metadata {
class Consumer;
}

class Formatter;

/**
 * Metadata information about a message
 */
struct Metadata : public ItemSet
{
protected:
	std::string m_filename;
	std::string m_notes;

	void clear();

public:
	UItem<types::Source> source;

protected:
	/**
	 * Inline data, or cached version of previously read data
	 */
	mutable wibble::sys::Buffer m_inline_buf;

	// Empty this Metadata object
	void reset();

public:
	Metadata() {}
	~Metadata();

	std::vector< Item<types::Note> > notes() const;
	const std::string& notes_encoded() const;
	void set_notes(const std::vector< Item<types::Note> >& notes);
	void set_notes_encoded(const std::string& notes);
	void add_note(const Item<types::Note>& note);

	/**
	 * Check that two Metadata contain the same information
	 */
	bool operator==(const Metadata& m) const;

	/**
	 * Check that two Metadata contain different information
	 */
	bool operator!=(const Metadata& m) const { return !operator==(m); }

	int compare(const Metadata& m) const;
	bool operator<(const Metadata& o) const { return compare(o) < 0; }
	bool operator<=(const Metadata& o) const { return compare(o) <= 0; }
	bool operator>(const Metadata& o) const { return compare(o) > 0; }
	bool operator>=(const Metadata& o) const { return compare(o) >= 0; }

	/**
	 * Create a new, empty in-memory metadata document
	 */
	void create();

	/**
	 * Read a metadata document from the given memory buffer
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * If readInline is true, in case the data is transmitted inline, it reads
	 * the data as well: this is what you expect.
	 *
	 * If it's false, then the reader needs to check from the Metadata source
	 * if it is inline, and in that case proceed to read the inline data.
	 *
	 * @returns false when the end of the buffer is reached
	 */
	bool read(const unsigned char*& buf, size_t& len, const std::string& filename);

	/**
	 * Decode the metadata, without the outer bundle headers, from the given buffer.
	 */
	void read(const unsigned char* buf, size_t len, unsigned version, const std::string& filename);

	/**
	 * Decode the metadata, without the outer bundle headers, from the given buffer.
	 */
	void read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename);

	/**
	 * Read a metadata document from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * If readInline is true, in case the data is transmitted inline, it reads
	 * the data as well: this is what you expect.
	 *
	 * If it's false, then the reader needs to check from the Metadata source
	 * if it is inline, and in that case proceed to read the inline data.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool read(std::istream& in, const std::string& filename, bool readInline = true);

	/**
	 * Read the inline data from the given stream
	 */
	void readInlineData(std::istream& in, const std::string& filename);

	/**
	 * Read a metadata document encoded in Yaml from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool readYaml(std::istream& in, const std::string& filename);

	/**
	 * Write the metadata to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(std::ostream& out, const std::string& filename) const;

	/**
	 * Write the metadata to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(int outfd, const std::string& filename) const;

	/**
	 * Write the metadata as YAML text to the given output stream.
	 */
	void writeYaml(std::ostream& out, const Formatter* formatter = 0) const;

	/**
	 * Encode to a string
	 */
	std::string encode() const;


	/**
	 * Read the raw blob data described by this metadata.
	 *
	 * Optionally, an input directory can be given as a base to resolve
	 * relative paths.
	 */
	wibble::sys::Buffer getData() const;

	/**
	 * If \a pathname is absolute, return it. Else, prepend to it this
	 * metadata position on the file system
	 */
	std::string completePathname(const std::string& pathname) const;

	/**
	 * If the source is not inline, but the data are cached in memory, drop
	 * them.
	 *
	 * Data for non-inline metadata can be cached in memory, for example,
	 * by a getData() call or a setCachedData() call.
	 */
	void dropCachedData();

	/**
	 * Set cached data for non-inline sources, so that getData() won't have
	 * to read it again.
	 */
	void setCachedData(const wibble::sys::Buffer& buf);

	/**
	 * Set the inline data for the metadata
	 */
	void setInlineData(const std::string& format, const wibble::sys::Buffer& buf);

	/**
	 * Remove the cached data from the metadata
	 *
	 * Note: this will do nothing if the source type is INLINE
	 */
	void resetInlineData();

	/**
	 * Read the data and inline them in the metadata
	 */
	void makeInline();

	/**
	 * Return the size of the data, if known
	 */
	size_t dataSize() const;

	/**
	 * Prepend this path to the filename in the blob source.
	 *
	 * If the source style is not blob, throws an exception.
	 */
	void prependPath(const std::string& path);

	/**
	 * Read all metadata from a file into the given consumer
	 */
	static void readFile(const std::string& fname, metadata::Consumer& mdc);

	/**
	 * Read a metadata group into the given consumer
	 */
	static void readGroup(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename, metadata::Consumer& mdc);

	/**
	 * Flush open data readers.
	 *
	 * Metadata uses a persistent data reader to read data, which keeps the
	 * last file opened and buffered to speed up reading multiple data
	 * items from the same file. This function tells the data reader to
	 * close its open files.
	 *
	 * It is useful for testing cases when data files are moved or
	 * compressed.
	 */
	static void flushDataReaders();

	/**
	 * Read all metadata from a file into the given consumer
	 */
	static void readFile(std::istream& in, const std::string& fname, metadata::Consumer& mdc);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Metadata userdata
	 *
	 * @return the Metadata element, or 0 if the check failed
	 */
	static Metadata* lua_check(lua_State* L, int idx);

	/**
	 * Load metadata functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);
};

}

// vim:set ts=4 sw=4:
#endif
