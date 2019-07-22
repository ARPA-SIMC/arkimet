#ifndef ARKI_MATCHER_ALIASES_H
#define ARKI_MATCHER_ALIASES_H

#include <map>
#include <string>
#include <memory>
#include <arki/core/fwd.h>
#include <arki/matcher/fwd.h>

namespace arki {
namespace matcher {

/// Container for all the matcher subexpressions for one type
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


/**
 * Store aliases to be used by the matcher
 */
struct AliasDatabase
{
    std::map<std::string, matcher::Aliases> aliasDatabase;

    AliasDatabase();
    AliasDatabase(const core::cfg::Sections& cfg);

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


class AliasDatabaseOverride
{
protected:
    AliasDatabase newdb;
    AliasDatabase* orig;

public:
    AliasDatabaseOverride();
    AliasDatabaseOverride(const core::cfg::Sections& cfg);
    AliasDatabaseOverride(const AliasDatabaseOverride&) = delete;
    AliasDatabaseOverride(AliasDatabaseOverride&&) = delete;
    AliasDatabaseOverride& operator=(const AliasDatabaseOverride&) = delete;
    AliasDatabaseOverride& operator=(AliasDatabaseOverride&&) = delete;
    ~AliasDatabaseOverride();
};


}
}

#endif
