#ifndef ARKI_MATCHER_PARSER_H
#define ARKI_MATCHER_PARSER_H

/// Metadata match expressions
#include <arki/matcher.h>

namespace arki {
namespace matcher {

class Parser
{
protected:
    AliasDatabase* aliases = nullptr;

public:
    Parser();
    Parser(const Parser&) = delete;
    Parser(Parser&&);
    ~Parser();

    Parser& operator=(const Parser&) = delete;
    Parser& operator=(Parser&&);

    Matcher parse(const std::string& pattern) const;

    /**
     * Read the system Matcher alias database.
     *
     * The file given in the environment variable ARKI_ALIASES is tried.
     * Else, $(sysconfdir)/arkimet/match-alias.conf is tried.
     * Else, nothing is loaded.
     */
    void load_system_aliases();
};

}
}

#endif

