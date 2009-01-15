#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/*
 * metadata - Handle xgribarch metadata
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
#include <arki/types/note.h>
#include <arki/types/source.h>
#include <wibble/sys/buffer.h>
#include <set>
#include <string>

struct lua_State;

namespace arki {

class Formatter;
class MetadataConsumer;

/**
 * Metadata information about a message
 */
struct Metadata
{
protected:
	std::map< types::Code, Item<> > m_vals;

	std::string m_filename;

public:
	typedef std::map< types::Code, Item<> >::const_iterator const_iterator;

	const_iterator begin() const { return m_vals.begin(); }
	const_iterator end() const { return m_vals.end(); }

	bool deleted;

	std::vector< Item<types::Note> > notes;

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

	UItem<> get(types::Code code) const;

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

	/// Set an item
	void set(const Item<>& i);

	/// Unset an item
	void unset(types::Code code);

	/// Set an item
	template<typename T>
	void set(const Item<T>& i) { set(Item<>(i)); }

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
	 * Set the inline data for the metadata
	 */
	void setInlineData(const std::string& format, const wibble::sys::Buffer& buf);

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
	 * Read all metadata from a file into a vector
	 */
	static std::vector<Metadata> readFile(const std::string& fname);

	/**
	 * Read all metadata from a file into the given consumer
	 */
	static void readFile(const std::string& fname, MetadataConsumer& mdc);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Origin LUA object
	static int lua_lookup(lua_State* L);
};

/**
 * Generic interface for metadata consumers, used to handle a stream of
 * metadata, such as after scanning a file, or querying a dataset.
 */
struct MetadataConsumer
{
	virtual ~MetadataConsumer() {}
	/**
	 * Consume a metadata.
	 *
	 * If the result is true, then the consumer is happy to accept more
	 * metadata.  If it's false, then the consume is satisfied and must not be
	 * sent any more metadata.
	 */
	virtual bool operator()(Metadata&) = 0;
};

/**
 * Turn a stream of bytes into a stream of metadata
 */
class MetadataStream
{
	MetadataConsumer& consumer;
	Metadata md;
	std::string streamname;
	std::string buffer;
	enum { METADATA, DATA } state;
	size_t dataToGet;

	void checkMetadata();
	void checkData();

public:
	MetadataStream(MetadataConsumer& consumer, const std::string& streamname)
		: consumer(consumer), streamname(streamname), state(METADATA) {}

	/**
	 * Return the number of bytes that have not been processed yet
	 */
	size_t countBytesUnprocessed() const { return buffer.size(); }

	/**
	 * Send some data to the stream.
	 *
	 * If the data completes one or more metadata and (when appropriate) the
	 * attached inline data, then they will be sent to the consumer
	 */
	void readData(const void* buf, size_t size);
};

}

// vim:set ts=4 sw=4:
#endif
