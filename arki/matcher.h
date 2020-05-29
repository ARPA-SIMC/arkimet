#ifndef ARKI_MATCHER_H
#define ARKI_MATCHER_H

/// Metadata match expressions
#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/matcher/fwd.h>
#include <string>
#include <set>
#include <memory>

namespace arki {

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
    Matcher(std::unique_ptr<matcher::AND>&& impl);
    Matcher(std::shared_ptr<matcher::AND> impl);
    Matcher(const Matcher& val) = default;
    Matcher(Matcher&& val) = default;
    ~Matcher() {}

    /// Assignment
    Matcher& operator=(const Matcher& val) = default;
    Matcher& operator=(Matcher&& val) = default;

    bool empty() const;

    /// Use of the underlying pointer
    const matcher::AND* operator->() const;

    /// Matcher name: the name part in "name:expr"
    std::string name() const;

    bool operator()(const types::Type& t) const;

    /// Match a full ItemSet
    bool operator()(const types::ItemSet& md) const;

    std::shared_ptr<matcher::OR> get(types::Code code) const;

    void foreach_type(std::function<void(types::Code, const matcher::OR&)> dest) const;

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
    std::string toString() const;

    /**
     * Format back into a string that can be parsed again, with all
     * aliases expanded
     */
    std::string toStringExpanded() const;

    /// Return a matcher matching a time interval (from begin included, to end excluded)
    static Matcher match_interval(const core::Time& begin, const core::Time& end);

    /// Return a matcher matching a whole month
    static Matcher match_month(unsigned year, unsigned month);

//private:
    /// Parse a string into a matcher
    static Matcher parse(const std::string& pattern);
};

/// Write as a string to an output stream
std::ostream& operator<<(std::ostream& o, const Matcher& m);

}

#endif
