#ifndef ARKI_SUMMARY_H
#define ARKI_SUMMARY_H

/*
 * summary - Handle a summary of a group of summary
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


#include <arki/types.h>
#include <arki/types/reftime.h>
#include <arki/utils/geosfwd.h>
#include <map>
#include <string>
#include <memory>
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

namespace summary {
class LuaIter;

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

	void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	std::ostream& writeToOstream(std::ostream& o) const;
	std::string toYaml(size_t indent = 0) const;
	void toYaml(std::ostream& out, size_t indent = 0) const;
	static arki::Item<Stats> decode(const unsigned char* buf, size_t len, const std::string& filename);
	static arki::Item<Stats> decodeString(const std::string& str);

	virtual void lua_push(lua_State* L) const;
	static int lua_lookup(lua_State* L);
};

struct Visitor
{
	virtual ~Visitor() {}
	virtual bool operator()(const std::vector< UItem<> >& md, const arki::Item<Stats>& stats) = 0;

	/// Return the metadata code for a given md vector position
	static types::Code codeForPos(size_t pos);

	/**
	 * Return md vector position for a given code
	 *
	 * Returns -1 if the given type code is not included in summaries
	 */
	static int posForCode(types::Code code);
};

struct StatsVisitor
{
	virtual ~StatsVisitor() {}
	virtual bool operator()(const arki::Item<Stats>& stats) = 0;
};

struct ItemVisitor
{
	virtual ~ItemVisitor() {}
	virtual bool operator()(const arki::UItem<>& item) = 0;
};

struct Node : public refcounted::Base
{
	// Metadata represented here
	std::vector< UItem<> > md;
	// Children representing the rest of the metadata
	// TODO: replace with vector, insertionsort, binary search
	std::map< UItem<>, refcounted::Pointer<Node> > children;
	// Statistics about the metadata scanned so far
	UItem<Stats> stats;

	Node();
	// New node initialized from the given metadata
	Node(const Metadata& m, size_t scanpos = 0);
	Node(const Metadata& m, const arki::Item<Stats>& st, size_t scanpos = 0);
	Node(const std::vector< UItem<> >& m, const arki::Item<Stats>& st, size_t scanpos = 0);
	virtual ~Node();

	// Visit all the contents of this node, notifying visitor of all the full
	// nodes found
	bool visit(Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos = 0) const;

	// Visit all the stats contained in this node and all subnotes
	bool visitStats(StatsVisitor& visitor) const;

	// Notifies the visitor of all the values of the given metadata item.
	// Due to the internal structure, the same item can be notified more than once.
	bool visitItem(size_t msoidx, ItemVisitor& visitor) const;

	// Visit all the contents of this node, notifying visitor of all the full
	// nodes found that match the matcher
	bool visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos = 0) const;

	// Add a metadata item
	void add(const Metadata& m, size_t scanpos = 0);
	void add(const Metadata& m, const arki::Item<Stats>& st, size_t scanpos = 0);
	void add(const std::vector< UItem<> >& m, const arki::Item<Stats>& st, size_t scanpos = 0);

	// Return the total size of all the metadata described by this node.
	unsigned long long size() const;

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
	std::auto_ptr<ARKI_GEOS_GEOMETRY> getConvexHull() const;

	virtual void encode(utils::codec::Encoder& enc, const UItem<>& lead = UItem<>(), size_t scanpos = 0) const;

	int compare(const Node& node) const;

	static refcounted::Pointer<Node> decode(const unsigned char* buf, size_t len, size_t scanpos = 0);
};

}

/**
 * Summary information of a bundle of summary
 */
class Summary
{
protected:
	std::string m_filename;

	refcounted::Pointer<summary::Node> root;

public:
	Summary() {}
	~Summary() {}

	/**
	 * Check that two Summary contain the same information
	 */
	bool operator==(const Summary& m) const;

	/**
	 * Check that two Summary contain different information
	 */
	bool operator!=(const Summary& m) const { return !operator==(m); }

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
	 * Read a summary from a POSIX file descriptor.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool read(int fd, const std::string& filename);

	/**
	 * Read a summary from the given input stream.
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
	 * Read data from the given file
	 */
	void readFile(const std::string& fname);

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
	 * Write the summary to the given file name.
	 *
	 * The file will be created with a temporary name, then renamed to the
	 * final name.
	 *
	 * \warn The temporary file name will NOT be created securely.
	 */
	void writeAtomically(const std::string& filename);

	/**
	 * Write the summary as YAML text to the given output stream.
	 */
	void writeYaml(std::ostream& out, const Formatter* f = 0) const;
	
	/**
	 * Encode to a string
	 */
	std::string encode() const;

	/**
	 * Check if this summary matches the given matcher
	 */
	bool match(const Matcher& matcher) const;

	/**
	 * Create a new summary with only those items that are matched by the
	 * matcher
	 */
	Summary filter(const Matcher& matcher) const;

	/**
	 * Add to summary those items that are matched by the matcher
	 */
	void filter(const Matcher& matcher, Summary& result) const;

	/**
	 * Add information about this metadata to the summary
	 */
	void add(const Metadata& md);

	/**
	 * Add information about several metadata to the summary: the
	 * summarisable metadata items are taken from 'md', and the statistics
	 * from 'st'
	 */
	void add(const Metadata& md, const arki::Item<summary::Stats>& st);

	/**
	 * Merge a summary into this summary
	 */
	void add(const Summary& s);

	/**
	 * Visit all the contents of this summary
	 *
	 * Returns true if the visit was completed, false if the visitor
	 * aborted the visit.
	 */
	bool visit(summary::Visitor& visitor) const;

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
	std::auto_ptr<ARKI_GEOS_GEOMETRY> getConvexHull() const;

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Summary userdata
	 *
	 * @return the Summary element, or 0 if the check failed
	 */
	static Summary* lua_check(lua_State* L, int idx);

	/**
	 * Load summary functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);

	friend class matcher::AND;
};

std::ostream& operator<<(std::ostream& o, const Summary& s);

}

// vim:set ts=4 sw=4:
#endif
