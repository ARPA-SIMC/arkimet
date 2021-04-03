#ifndef ARKI_MATCHER_ALIASES_H
#define ARKI_MATCHER_ALIASES_H

#include <map>
#include <string>
#include <memory>
#include <arki/core/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/stream/fwd.h>

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
    void validate(const Aliases& other);
    void add(const MatcherType& type, const core::cfg::Section& entries);
    void reset();
    void serialise(core::cfg::Section& cfg) const;
};


/**
 * Store aliases to be used by the matcher
 */
class AliasDatabase
{
public:
    std::map<std::string, matcher::Aliases> aliasDatabase;

    AliasDatabase();
    AliasDatabase(const core::cfg::Sections& cfg);

    /**
     * Validate an alias database, throwing std::runtime_error if it contains
     * aliases with the same name as the existing ones, but different
     * expansions
     */
    void validate(const core::cfg::Sections& cfg);

    /**
     * Add an alias database.
     *
     * If an alias already exists, it is replaced
     */
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
    stream::SendResult debug_dump(StreamOutput& out);
};


/**
 * Read the alias database from the given remote dataset
 */
std::shared_ptr<core::cfg::Sections> load_remote_alias_database(const std::string& server);


}
}

#endif
