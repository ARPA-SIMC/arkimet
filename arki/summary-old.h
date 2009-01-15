#ifndef ARKI_SUMMARY_OLD_H
#define ARKI_SUMMARY_OLD_H

/*
 * summary - Handle a summary of a group of summary
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
#include <arki/types/reftime.h>
#include <arki/types/bbox.h>
#include <map>
#include <string>
#include <iosfwd>

struct lua_State;

namespace wibble {
namespace sys {
struct Buffer;
}
}

namespace arki {
class Metadata;
class Matcher;
class Formatter;

namespace types {
class Origin;
class Product;
class Level;
class Timerange;
class Area;
class Ensemble;
}

namespace matcher {
class AND;
}

namespace oldsummary {
class LuaIter;

struct Item : public types::Type, public std::map< types::Code, arki::Item<> >
{
	Item() {}

	virtual int compare(const Type& o) const;
	virtual int compare(const Item& o) const;
	virtual bool operator==(const Type& o) const;
	virtual bool operator==(const Item& o) const;

	UItem<> get(types::Code code) const;

	virtual std::string tag() const;
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;

	std::string encodeWithoutEnvelope() const;
	std::ostream& writeToOstream(std::ostream& o) const;
	std::string toYaml(size_t indent = 0, const Formatter* f = 0) const;
	void toYaml(std::ostream& out, size_t indent = 0, const Formatter* f = 0) const;
	static arki::Item<Item> decode(const unsigned char* buf, size_t len, const std::string& filename);
	static arki::Item<Item> decodeString(const std::string& str);

	virtual void lua_push(lua_State* L) const;
	static int lua_lookup(lua_State* L);

	static bool accepts(types::Code);
};

struct Stats : public types::Type
{
	size_t count;
	unsigned long long size;
	types::reftime::Collector reftimeMerger;

	Stats() : count(0), size(0) {}
	Stats(size_t count, unsigned long long size, const types::Reftime* reftime);

	virtual int compare(const Type& o) const;
	virtual int compare(const Stats& o) const;
	virtual bool operator==(const Type& o) const;
	virtual bool operator==(const Stats& o) const;

	bool operator!=(const Stats& i) const { return !operator==(i); }
	bool operator<(const Stats& i) const { return compare(i) < 0; }

	void merge(const arki::Item<Stats>& s);
	void merge(size_t count, unsigned long long size, const types::Reftime* reftime);

	virtual std::string tag() const;
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;

	std::string encodeWithoutEnvelope() const;
	std::ostream& writeToOstream(std::ostream& o) const;
	std::string toYaml(size_t indent = 0) const;
	void toYaml(std::ostream& out, size_t indent = 0) const;
	static arki::Item<Stats> decode(const unsigned char* buf, size_t len, const std::string& filename);
	static arki::Item<Stats> decodeString(const std::string& str);

	virtual void lua_push(lua_State* L) const;
	static int lua_lookup(lua_State* L);
};

}

/**
 * Summary information of a bundle of summary
 */
class OldSummary
{
protected:
	std::string m_filename;

	typedef std::map< Item<oldsummary::Item>, Item<oldsummary::Stats> > map_t;
	// TODO: for performance, this could be a trie-like tree,
	//       most frequent item on top
	//       In the meantime, this could just be a set
	map_t m_items;

	// Empty this Summary object
	void reset();

public:
	OldSummary() {}
	~OldSummary();

	typedef map_t::const_iterator const_iterator;

	const_iterator begin() const { return m_items.begin(); }
	const_iterator end() const { return m_items.end(); }

	/**
	 * Check that two Summary contain the same information
	 */
	bool operator==(const OldSummary& m) const;

	/**
	 * Check that two Summary contain different information
	 */
	bool operator!=(const OldSummary& m) const { return !operator==(m); }

	/**
	 * Create a new, empty in-memory summary document
	 */
	void clear();

	/**
	 * Return the number of metadata described by this summary.
	 */
	size_t count() const;

	/**
	 * Return the total size of all the metadata described by this summary.
	 */
	unsigned long long size() const;

	/**
	 * Read a summary document from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool read(std::istream& in, const std::string& filename);
	
	/**
	 * Decode the summary, without the outer bundle headers, from the given buffer.
	 */
	void read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename);

	/**
	 * Read a summary document encoded in Yaml from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * Summary items are read from the file until the end of file is found.
	 */
	bool readYaml(std::istream& in, const std::string& filename);

	/**
	 * Write the summary to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(std::ostream& out, const std::string& filename) const;

	/**
	 * Write the summary as YAML text to the given output stream.
	 */
	void writeYaml(std::ostream& out, const Formatter* f = 0) const;

	/**
	 * Encode to a string
	 */
	std::string encode() const;

	/**
	 * Add information about this metadata to the summary
	 */
	void add(const Metadata& md);

	/**
	 * Add this summary item to the summary
	 */
	void add(const Item<oldsummary::Item>& i, const Item<oldsummary::Stats>& s);

	/**
	 * Merge a summary into this summary
	 */
	void add(const OldSummary& s);

	/**
	 * Get the reference time interval covered by the metadata bundle.
	 *
	 * Note: an end period of (0, 0, 0, 0, 0, 0) means "now".
	 */
	Item<types::Reftime> getReferenceTime() const;

	/**
	 * Get the convex hull of the union of all bounding boxes covered by the
	 * metadata bundle.
	 */
	Item<types::BBox> getConvexHull() const;

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Origin LUA object
	static int lua_lookup(lua_State* L);

	friend class matcher::AND;
};

std::ostream& operator<<(std::ostream& o, const OldSummary& s);

}

// vim:set ts=4 sw=4:
#endif
