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

    const matcher::Aliases* get(const std::string& type) const;

    std::shared_ptr<core::cfg::Sections> serialise();

    /**
     * Dump the alias database to the given output stream
     *
     * (used for debugging purposes)
     */
    void debug_dump(core::NamedFileDescriptor& out);

    /**
     * Dump the alias database to the given output stream
     *
     * (used for debugging purposes)
     */
    void debug_dump(core::AbstractOutputFile& out);
};


/**
 * Read the alias database from the given remote dataset
 */
std::shared_ptr<core::cfg::Sections> load_remote_alias_database(const std::string& server);


}
}

#endif
