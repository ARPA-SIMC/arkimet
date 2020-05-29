#ifndef ARKI_MATCHER_UTILS
#define ARKI_MATCHER_UTILS

/*
 * matcher/utils - Support code to implement matchers
 *
 * Copyright (C) 2007,2008  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <arki/matcher.h>
#include <arki/utils/string.h>
#include <vector>
#include <string>
#include <map>
#include <cstdint>

namespace arki {
namespace matcher {

struct Implementation;


/**
 * This class is used to register matchers with arkimet
 *
 * Registration is done by declaring a static MatcherType object, passing the
 * matcher details in the constructor.
 */
struct MatcherType
{
    typedef std::unique_ptr<Implementation> (*subexpr_parser)(const std::string& pattern);

    std::string name;
    types::Code code;
    subexpr_parser parse_func;

    MatcherType(const std::string& name, types::Code code, subexpr_parser parse_func);
    ~MatcherType();

    static MatcherType* find(const std::string& name);
    static std::vector<std::string> matcherNames();

    /// Register a matcher type
    static void register_matcher(const std::string& name, types::Code code, matcher::MatcherType::subexpr_parser parse_func);
};


/**
 * Base class for implementing arkimet matchers
 */
class Implementation
{
public:
    Implementation() {}
    Implementation(const Implementation&) = delete;
    Implementation(const Implementation&&) = delete;
    Implementation& operator=(const Implementation&) = delete;
    Implementation& operator=(const Implementation&&) = delete;
    virtual ~Implementation() {}

    /// Matcher name: the name part in "name:expr"
    virtual std::string name() const = 0;

    /// Match a metadata item
    virtual bool matchItem(const types::Type& t) const = 0;

    /// Format back into a string that can be parsed again
    virtual std::string toString() const = 0;

    /**
     * Format back into a string that can be parsed again, with all aliases
     * expanded.
     */
    virtual std::string toStringExpanded() const { return toString(); }
};


/// ORed list of matchers, all of the same type, with the same name
class OR : public Implementation
{
protected:
    std::vector<std::shared_ptr<Implementation>> components;

public:
    std::string unparsed;

    OR(const std::string& unparsed) : unparsed(unparsed) {}
    virtual ~OR();

    std::string name() const override;
    bool matchItem(const types::Type& t) const override;

    // Serialise as "type:original definition"
    std::string toString() const override;
    // Serialise as "type:expanded definition"
    std::string toStringExpanded() const override;
    // Serialise as "original definition" only
    std::string toStringValueOnly() const;
    // Serialise as "expanded definition" only
    std::string toStringValueOnlyExpanded() const;

    // If we match Reftime elements, build a SQL query for it. Else, throw an exception.
    std::string toReftimeSQL(const std::string& column) const;

    bool intersect_interval(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;

    static std::unique_ptr<OR> parse(const Aliases* aliases, const MatcherType& type, const std::string& pattern);
    static std::unique_ptr<OR> wrap(std::unique_ptr<Implementation> impl);
};


/// ANDed list of matchers.
class AND : public Implementation
{
protected:
    std::map<types::Code, std::shared_ptr<OR>> components;

public:
    //typedef std::map< types::Code, refcounted::Pointer<const Implementation> >::const_iterator const_iterator;

    AND() {}
    virtual ~AND();

    bool empty() const { return components.empty(); }

    std::string name() const override;

    bool matchItem(const types::Type& t) const override;
    bool matchItemSet(const types::ItemSet& s) const;

    std::shared_ptr<OR> get(types::Code code) const;

    void foreach_type(std::function<void(types::Code, const OR&)> dest) const;

    void split(const std::set<types::Code>& codes, AND& with, AND& without) const;

    std::string toString() const override;
    std::string toStringExpanded() const override;

    static std::unique_ptr<AND> parse(const AliasDatabase& aliases, const std::string& pattern);
    static std::unique_ptr<AND> match_interval(const core::Time& begin, const core::Time& end);
};


struct OptionalCommaList : public std::vector<std::string>
{
	std::string tail;

	OptionalCommaList(const std::string& pattern, bool has_tail=false);

    bool has(size_t pos) const;
    int getInt(size_t pos, int def) const;
    unsigned getUnsigned(size_t pos, unsigned def) const;
    uint32_t getUnsignedWithMissing(size_t pos, uint32_t missing, bool& has_val) const;
    double getDouble(size_t pos, double def) const;
    const std::string& 	getString	(size_t pos, const std::string& def) const;

#if 0
	bool matchInt(size_t pos, int val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtoul((*this)[pos].c_str(), 0, 10);
		return req == val;
	}

	bool matchDouble(size_t pos, double val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtod((*this)[pos].c_str(), 0);
		return req == val;
	}

	std::string toString() const
	{
		string res;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (res.empty())
				res += *i;
			else
				res += "," + *i;
		return res;
	}

	template<typename V>
	bool matchVals(const V* vals, size_t size) const
	{
		for (size_t i = 0; i < size; ++i)
			if (this->size() > i && !(*this)[i].empty() && (V)strtoul((*this)[i].c_str(), 0, 10) != vals[i])
				return false;
		return true;
	}

	std::string make_sql(const std::string& column, const std::string& style, size_t maxvals) const
	{
		string res = column + " LIKE '" + style;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
		{
			res += ',';
			if (i->empty())
				res += '%';
			else
				res += *i;
		}
		if (size() < maxvals)
			res += ",%";
		return res + '\'';
	}

	std::vector< std::pair<int, bool> > getIntVector() const
	{
		vector< pair<int, bool> > res;

		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (i->empty())
				res.push_back(make_pair(0, false));
			else
				res.push_back(make_pair(strtoul(i->c_str(), 0, 10), true));

		return res;
	}
#endif
};

struct CommaJoiner : std::vector<std::string>
{
	size_t last;

	CommaJoiner() : last(0) {}

    template<typename T>
    void add(const T& val)
    {
        std::stringstream ss;
        ss << val;
        push_back(ss.str());
        last = size();
    }

    template<typename T>
    void add(const T& val, const T& missing)
    {
        if (val == missing)
            push_back("-");
        else
        {
            std::stringstream ss;
            ss << val;
            push_back(ss.str());
        }
        last = size();
    }

	void addUndef()
	{
		push_back(std::string());
	}

	std::string join() const
	{
		std::string res;
		for (size_t i = 0; i < last; ++i)
			if (res.empty())
				res += (*this)[i];
			else
				res += "," + (*this)[i];
		return res;
	}
};

}
}

// vim:set ts=4 sw=4:
#endif
