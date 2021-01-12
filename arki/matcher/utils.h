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

#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <arki/matcher/fwd.h>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <set>

namespace arki {
namespace matcher {

class Implementation;


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
    Implementation(const Implementation&) = default;
    Implementation(Implementation&&) = delete;
    Implementation& operator=(const Implementation&) = delete;
    Implementation& operator=(const Implementation&&) = delete;
    virtual ~Implementation() {}

    /// Return a newly allocated copy of this Implementation
    virtual Implementation* clone() const = 0;

    /// Matcher name: the name part in "name:expr"
    virtual std::string name() const = 0;

    /// Match a metadata item
    virtual bool matchItem(const types::Type& t) const = 0;

    /// Match a metadata item
    virtual bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const;

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

    OR* clone() const override;

    std::string name() const override;
    bool matchItem(const types::Type& t) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    bool match_interval(const core::Interval& interval) const;

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

    bool intersect_interval(core::Interval& interval) const;

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

    AND* clone() const override;

    bool empty() const { return components.empty(); }

    std::string name() const override;

    bool matchItem(const types::Type& t) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    bool matchItemSet(const types::ItemSet& s) const;
    bool matchMetadata(const Metadata& s) const;
    bool match_interval(const core::Interval& interval) const;

    std::shared_ptr<OR> get(types::Code code) const;

    void foreach_type(std::function<void(types::Code, const OR&)> dest) const;

    void split(const std::set<types::Code>& codes, AND& with, AND& without) const;

    std::string toString() const override;
    std::string toStringExpanded() const override;

    static std::unique_ptr<AND> parse(const AliasDatabase& aliases, const std::string& pattern);
    static std::unique_ptr<AND> for_interval(const core::Interval& interval);
};


template<typename T>
struct Optional
{
    bool present = false;
    T value;

    Optional() = default;
    Optional(const T& value) : present(true), value(value) {}
    Optional(const Optional&) = default;
    Optional(Optional&&) = default;
    Optional& operator=(const Optional&) = default;
    Optional& operator=(Optional&&) = default;
    template<typename T1>
    Optional& operator=(const Optional<T1>& o)
    {
        if (this == reinterpret_cast<const Optional*>(&o))
            return *this;
        present = o.present;
        value = o.value;
        return *this;
    }

    void set(const T& value)
    {
        present = true;
        this->value = value;
    }

    void unset()
    {
        present = false;
    }

    /**
     * Return True if value is not present. If it is present, return true if
     * it is the same as the passed value
     */
    bool matches(const T& value) const
    {
        if (!present)
            return true;

        return this->value == value;
    }
};

template<typename T>
std::ostream& operator<<(std::ostream& o, const Optional<T>& v)
{
    if (v.present)
        return o << v.value;
    else
        return o << "(undefined)";
}


struct OptionalCommaList : public std::vector<std::string>
{
	std::string tail;

	OptionalCommaList(const std::string& pattern, bool has_tail=false);

    bool has(size_t pos) const;
    int getInt(size_t pos, int def) const;
    unsigned getUnsigned(size_t pos, unsigned def) const;
    uint32_t getUnsignedWithMissing(size_t pos, uint32_t missing, bool& has_val) const;
    Optional<uint32_t> getUnsignedWithMissing(size_t pos, uint32_t missing) const;
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
    void add(const Optional<T>& val)
    {
        if (!val.present)
            addUndef();
        else
            push_back(std::to_string(val.value));
        last = size();
    }

    void add(const uint8_t val)
    {
        push_back(std::to_string((unsigned)val));
        last = size();
    }
    void add(const char* val)
    {
        push_back(val);
        last = size();
    }

    template<typename T>
    void add(const T& val)
    {
        std::stringstream ss;
        ss << val;
        push_back(ss.str());
        last = size();
    }

    template<typename T>
    void add(const Optional<T>& val, const T& missing)
    {
        if (!val.present)
            addUndef();
        else if (val.value == missing)
            add_missing();
        else
            add(val.value);
    }

    template<typename T>
    void add(const T& val, const T& missing)
    {
        if (val == missing)
            add_missing();
        else
            add(val);
    }

    void add_missing()
    {
        push_back("-");
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

#endif
