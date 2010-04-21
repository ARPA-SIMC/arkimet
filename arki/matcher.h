#ifndef ARKI_MATCHER_H
#define ARKI_MATCHER_H

/*
 * matcher - Match metadata expressions
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
#include <wibble/exception.h>
#include <string>
#include <vector>
#include <map>
#include <set>

struct lua_State;

namespace arki {
namespace types {
struct Time;
}


class Metadata;
class Summary;
class ConfigFile;

/**
 * Represent a set of items to match.
 *
 * The integers are the serialisation codes of the items matched.
 */
struct MatchedItems : public std::set<int>
{
};

#if 0
/// Match type constants
enum MatchType
{
	MATCH_ORIGIN    =  1,
	MATCH_PRODUCT   =  2,
	MATCH_LEVEL     =  4,
	MATCH_TIMERANGE =  8,
	MATCH_REFTIME   = 16,
	MATCH_AREA      = 32,
	MATCH_ENSEMBLE  = 64,
};
#endif

namespace matcher {

struct MatcherType;

/**
 * Base class for implementing arkimet matchers
 */
class Implementation : public refcounted::Base
{
public:
	virtual ~Implementation() {}

	/// Numeric tag to identify this matcher type
	//virtual MatchType type() const = 0;

	/// Matcher name: the name part in "name:expr"
	virtual std::string name() const = 0;

	/// Match a metadata item
	virtual bool matchItem(const Item<>& t) const = 0;

	/// Format back into a string that can be parsed again
	virtual std::string toString() const = 0;

	/**
	 * Format back into a string that can be parsed again, with all aliases
	 * expanded.
	 */
	virtual std::string toStringExpanded() const { return toString(); }

	/// Push to the LUA stack a function for this matcher
	//virtual void lua_push(lua_State* L) const = 0;
};

/// ORed list of matchers, all of the same type, with the same name
struct OR : public std::vector< refcounted::Pointer<const Implementation> >, public Implementation
{
	std::string unparsed;

	OR(const std::string& unparsed) : unparsed(unparsed) {}
	virtual ~OR() {}

	virtual std::string name() const;

	virtual bool matchItem(const Item<>& t) const;

	std::string toString() const;
	std::string toStringExpanded() const;

	static OR* parse(const MatcherType& type, const std::string& pattern);
};

/// ANDed list of matchers.
struct AND : public std::map< types::Code, refcounted::Pointer<const Implementation> >, public Implementation
{
	typedef std::map< types::Code, refcounted::Pointer<const Implementation> >::const_iterator const_iterator;

	AND() {}
	virtual ~AND() {}

	virtual std::string name() const;

	virtual bool matchItem(const Item<>& t) const;
	virtual bool matchMetadata(const Metadata& s) const;

	const OR* get(types::Code code) const;

	void split(const std::set<types::Code>& codes, AND& with, AND& without) const;

	std::string toString() const;
	std::string toStringExpanded() const;

	static AND* parse(const std::string& pattern);
};

/**
 * This class is used to register matchers with arkimet
 *
 * Registration is done by declaring a static MatcherType object, passing the
 * matcher details in the constructor.
 */
struct MatcherType
{
	typedef Implementation* (*subexpr_parser)(const std::string& pattern);

	std::string name;
	types::Code code;
	subexpr_parser parse_func;
	
	MatcherType(const std::string& name, types::Code code, subexpr_parser parse_func);
	~MatcherType();

	static MatcherType* find(const std::string& name);
	static std::vector<std::string> matcherNames();
};

/// Namespace holding all the matcher subexpressions for one type
class Aliases
{
	std::map< std::string, const OR* > db;

public:
	~Aliases();

	const OR* get(const std::string& name) const;
	void add(const MatcherType& type, const ConfigFile& entries);
	void reset();
	void serialise(ConfigFile& cfg) const;
};

}

/**
 * Match metadata expressions.
 *
 * A metadata expression is a sequence of expressions separated by newlines or
 * semicolons.
 *
 * Every expression matches one part of a metadata.  It begins with the
 * expression type, followed by colon, followed by the value to match.
 *
 * Every expression of the matcher must be easily translatable into a SQL
 * query, in order to be able to efficiently use a Matcher with an index.
 *
 * Every subexpr can only match one kind of metadata, as the matcher must cope
 * with cases when some kinds of metadata are indexed and some are not.
 *
 * Example:
 *   origin:GRIB,,21
 *   reftime:>=2007-04-01,<=2007-05-10
 */
struct Matcher
{
	const matcher::AND* m_impl;

	Matcher() : m_impl(0) {}
	Matcher(const matcher::AND* impl) : m_impl(impl)
	{
		if (m_impl) m_impl->ref();
	}
	Matcher(const Matcher& val)
	{
		m_impl = val.m_impl;
		if (m_impl) m_impl->ref();
	}
	~Matcher()
	{
		if (m_impl && m_impl->unref())
			delete m_impl;
	}

	bool empty() const { return m_impl == 0 || m_impl->empty(); }

	/// Assignment
	Matcher& operator=(const Matcher& val)
	{
		if (val.m_impl) val.m_impl->ref();
		if (m_impl && m_impl->unref()) delete m_impl;
		m_impl = val.m_impl;
		return *this;
	}
	Matcher& operator=(const matcher::AND* val)
	{
		if (val) val->ref();
		if (m_impl && m_impl->unref()) delete m_impl;
		m_impl = val;
		return *this;
	}
	template<typename TYPE1>
	Matcher& operator=(TYPE1* val)
	{
		if (val) val->ref();
		if (m_impl && m_impl->unref()) delete m_impl;
		m_impl = val;
		return *this;
	}

	/// Use of the underlying pointer
	const matcher::AND* operator->() const
	{
		if (!m_impl)
			throw wibble::exception::Consistency("accessing matcher", "matcher is empty");
		return m_impl;
	}

	/// Numeric tag to identify this matcher type
	//virtual MatchType type() const = 0;

	/// Matcher name: the name part in "name:expr"
	std::string name() const
	{
		if (m_impl) return m_impl->name();
		return std::string();
	}

	bool operator()(const Item<>& t) const
	{
		if (m_impl) return m_impl->matchItem(t);
		// An empty matcher always matches
		return true;
	}

	/// Match a full metadata
	bool operator()(const Metadata& md) const
	{
		if (m_impl) return m_impl->matchMetadata(md);
		// An empty matcher always matches
		return true;
	}

	/**
	 * Match a summary.
	 *
	 * Return true if there is at least a metadata in this summary that is
	 * potentially matched by the matcher
	 */
	bool operator()(const Summary& i) const;

#if 0
	/// Match a collection of metadata items
	template<typename COLL>
	bool match(const COLL& s) const
	{
		for (typename COLL::const_iterator i = s.begin(); i != s.end(); ++i)
			if (match(*i))
				return true;
		return false;
	}
#endif

	void split(const std::set<types::Code>& codes, Matcher& with, Matcher& without) const;

	/**
	 * Compute the date extremes of this matcher
	 *
	 * @returns true if the range has at least one bound (i.e. either with
	 * or without are defined), false otherwise
	 */
	bool date_extremes(UItem<types::Time>& begin, UItem<types::Time>& end) const;

	/// Format back into a string that can be parsed again
	std::string toString() const
	{
		if (m_impl) return m_impl->toString();
		return std::string();
	}

	/**
	 * Format back into a string that can be parsed again, with all
	 * aliases expanded
	 */
	std::string toStringExpanded() const
	{
		if (m_impl) return m_impl->toStringExpanded();
		return std::string();
	}

	/// Parse a string into a matcher
	static Matcher parse(const std::string& pattern);


	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Summary userdata
	 *
	 * @return the Summary element, or 0 if the check failed
	 */
	static Matcher lua_check(lua_State* L, int idx);

	/**
	 * Load summary functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);
};

/// Write as a string to an output stream
std::ostream& operator<<(std::ostream& o, const Matcher& m);

/**
 * Store aliases to be used by the matcher
 */
struct MatcherAliasDatabase
{
	std::map<std::string, matcher::Aliases> aliasDatabase;

	MatcherAliasDatabase();
	MatcherAliasDatabase(const ConfigFile& cfg);

	void add(const ConfigFile& cfg);

	/**
	 * Add global aliases from the given config file.
	 *
	 * If there are already existing aliases, they are preserved unless cfg
	 * overwrites some of them.
	 *
	 * The aliases will be used by all newly instantiated Matcher expressions,
	 * for all the lifetime of the program.
	 */
	static void addGlobal(const ConfigFile& cfg);

	static const matcher::Aliases* get(const std::string& type);
	static const void reset();
	static void serialise(ConfigFile& cfg);
};

}

// vim:set ts=4 sw=4:
#endif
