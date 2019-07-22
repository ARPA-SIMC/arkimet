#ifndef ARKI_MATCHER_H
#define ARKI_MATCHER_H

/// Metadata match expressions
#include <arki/metadata/fwd.h>
#include <arki/types/fwd.h>
#include <arki/core/fwd.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <functional>

struct lua_State;

namespace arki {

/**
 * Represent a set of items to match.
 *
 * The integers are the serialisation codes of the items matched.
 */
struct MatchedItems : public std::set<int>
{
};

namespace matcher {

struct MatcherType;
struct Aliases;

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

    /**
     * Restrict date extremes to be no wider than what is matched by this
     * matcher.
     *
     * An unique_ptr set to NULL means an open end in the range. Date extremes
     * are inclusive on both ends.
     *
     * @returns true if the matcher has consistent reference time expressions,
     * false if the match is impossible (like reftime:<2014,>2015)
     */
    bool restrict_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;

    static std::unique_ptr<OR> parse(const MatcherType& type, const std::string& pattern);
    static std::unique_ptr<OR> parse(const MatcherType& type, const std::string& pattern, const Aliases* aliases);
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

    static std::unique_ptr<AND> parse(const std::string& pattern);
};

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
};

/// Namespace holding all the matcher subexpressions for one type
class Aliases
{
protected:
    std::map<std::string, std::shared_ptr<OR>> db;

public:
    ~Aliases();

    std::shared_ptr<OR> get(const std::string& name) const;
    void add(const MatcherType& type, const core::cfg::Section& entries);
    void reset();
    void serialise(core::cfg::Section& cfg) const;
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
class Matcher
{
    std::shared_ptr<matcher::AND> m_impl;

public:
    Matcher() {}
    Matcher(std::unique_ptr<matcher::AND>&& impl) : m_impl(move(impl)) {}
    Matcher(std::shared_ptr<matcher::AND> impl) : m_impl(impl) {}
    Matcher(const Matcher& val) = default;
    Matcher(Matcher&& val) = default;
    ~Matcher() {}

    /// Assignment
    Matcher& operator=(const Matcher& val) = default;
    Matcher& operator=(Matcher&& val) = default;

    bool empty() const { return m_impl.get() == 0 || m_impl->empty(); }

    /// Use of the underlying pointer
    const matcher::AND* operator->() const
    {
        if (!m_impl.get())
            throw std::runtime_error("cannot access matcher: matcher is empty");
        return m_impl.get();
    }

    /// Matcher name: the name part in "name:expr"
    std::string name() const
    {
        if (m_impl.get()) return m_impl->name();
        return std::string();
    }

    bool operator()(const types::Type& t) const
    {
        if (m_impl.get()) return m_impl->matchItem(t);
        // An empty matcher always matches
        return true;
    }

    /// Match a full ItemSet
    bool operator()(const types::ItemSet& md) const
    {
        if (m_impl.get()) return m_impl->matchItemSet(md);
        // An empty matcher always matches
        return true;
    }

    std::shared_ptr<matcher::OR> get(types::Code code) const
    {
        if (m_impl) return m_impl->get(code);
        return nullptr;
    }

    void foreach_type(std::function<void(types::Code, const matcher::OR&)> dest) const
    {
        if (!m_impl) return;
        return m_impl->foreach_type(dest);
    }

    void split(const std::set<types::Code>& codes, Matcher& with, Matcher& without) const;

    /**
     * Restrict date extremes to be no wider than what is matched by this
     * matcher.
     *
     * An unique_ptr set to NULL means an open end in the range. Date extremes
     * are inclusive on both ends.
     *
     * @returns true if the matcher has consistent reference time expressions,
     * false if the match is impossible (like reftime:<2014,>2015)
     */
    bool restrict_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;

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

    /// Register a matcher type
    static void register_matcher(const std::string& name, types::Code code, matcher::MatcherType::subexpr_parser parse_func);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Matcher
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Matcher userdata
	 *
	 * @return the Matcher element, or 0 if the check failed
	 */
	static Matcher lua_check(lua_State* L, int idx);

	/**
	 * Load summary functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);
};

/// Write as a string to an output stream
std::ostream& operator<<(std::ostream& o, const Matcher& m);

struct MatcherAliasDatabaseOverride;


/**
 * Store aliases to be used by the matcher
 */
struct MatcherAliasDatabase
{
    std::map<std::string, matcher::Aliases> aliasDatabase;

    MatcherAliasDatabase();
    MatcherAliasDatabase(const core::cfg::Sections& cfg);

    void add(const core::cfg::Sections& cfg);

    /**
     * Add global aliases from the given config file.
     *
     * If there are already existing aliases, they are preserved unless cfg
     * overwrites some of them.
     *
     * The aliases will be used by all newly instantiated Matcher expressions,
     * for all the lifetime of the program.
     */
    static void addGlobal(const core::cfg::Sections& cfg);

    static const matcher::Aliases* get(const std::string& type);
    static core::cfg::Sections serialise();

    /**
     * Dump the alias database to the given output stream
     *
     * (used for debugging purposes)
     */
    static void debug_dump(std::ostream& out);
};


class MatcherAliasDatabaseOverride
{
protected:
    MatcherAliasDatabase newdb;
    MatcherAliasDatabase* orig;

public:
    MatcherAliasDatabaseOverride();
    MatcherAliasDatabaseOverride(const core::cfg::Sections& cfg);
    MatcherAliasDatabaseOverride(const MatcherAliasDatabaseOverride&) = delete;
    MatcherAliasDatabaseOverride(MatcherAliasDatabaseOverride&&) = delete;
    MatcherAliasDatabaseOverride& operator=(const MatcherAliasDatabaseOverride&) = delete;
    MatcherAliasDatabaseOverride& operator=(MatcherAliasDatabaseOverride&&) = delete;
    ~MatcherAliasDatabaseOverride();
};

}

// vim:set ts=4 sw=4:
#endif
