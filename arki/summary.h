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
#include <arki/itemset.h>
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

namespace summary {
struct RootNode;
struct Stats;
}

namespace matcher {
class AND;
}

namespace summary {
class LuaIter;

struct Visitor
{
	virtual ~Visitor() {}
	virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats) = 0;

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
	virtual bool operator()(const Stats& stats) = 0;
};

struct ItemVisitor
{
	virtual ~ItemVisitor() {}
	virtual bool operator()(const arki::UItem<>& item) = 0;
};

}

/**
 * Summary information of a bundle of summary
 */
class Summary
{
protected:
    summary::RootNode* root;

public:
    Summary();
    Summary(const Summary& s);
    ~Summary();

    Summary& operator=(const Summary& s);

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

    /// Dump the internal structure of this summary for debugging purposes
    void dump(std::ostream& out) const;

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

    /// Decode from structured data
	void read(const emitter::memory::Mapping& val);

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

    /// Serialise using an emitter
    void serialise(Emitter& e, const Formatter* f=0) const;
	
	/**
	 * Encode to a string
	 */
	std::string encode(bool compressed = false) const;

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
	void add(const Metadata& md, const UItem<summary::Stats>& st);

	/**
	 * Merge a summary into this summary
	 */
	void add(const Summary& s);

	/**
	 * Merge a summary into this summary, keeping only the given metadata
	 * items
	 */
	void add(const Summary& s, const std::set<types::Code>& keep_only);

	/**
	 * Visit all the contents of this summary
	 *
	 * Returns true if the visit was completed, false if the visitor
	 * aborted the visit.
	 */
	bool visit(summary::Visitor& visitor) const;

	/**
	 * Visit all the contents of this summary
	 *
	 * Returns true if the visit was completed, false if the visitor
	 * aborted the visit.
	 */
	bool visitFiltered(const Matcher& matcher, summary::Visitor& visitor) const;

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
	std::auto_ptr<ARKI_GEOS_GEOMETRY> getConvexHull(ARKI_GEOS_GEOMETRYFACTORY& gf) const;

	/**
	 * Return all the unique combination of metadata items that are found
	 * by the matcher in this summary.
	 *
	 * Only metadata items for which there is an expression in matcher are
	 * present in the output.
	 */
	std::vector<ItemSet> resolveMatcher(const Matcher& matcher) const;

	/**
	 * Return all the unique combination of metadata items that are found
	 * by the matcher in this summary.
	 *
	 * Metadata are added to res, sorted and avoiding duplicated.
	 *
	 * Return the number of matching items found (0 if nothing matched)
	 */
	size_t resolveMatcher(const Matcher& matcher, std::vector<ItemSet>& res) const;

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
