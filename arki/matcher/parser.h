#ifndef ARKI_MATCHER_PARSER_H
#define ARKI_MATCHER_PARSER_H

/// Metadata match expressions
#include <arki/matcher.h>
#include <arki/stream/fwd.h>
#include <unordered_set>

namespace arki {
namespace matcher {

class Parser
{
protected:
    AliasDatabase* aliases = nullptr;
    std::unordered_set<std::string> servers_seen;

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

    /// Load aliases from the given parsed configuration file
    void load_aliases(const core::cfg::Sections& cfg);

    /**
     * Load aliases from a remote arki-server.
     *
     * If aliasese have already been loaded from that server, it does nothing.
     */
    void load_remote_aliases(const std::string& server_url);

    /// Return aliases serialized as a parsed config file
    std::shared_ptr<core::cfg::Sections> serialise_aliases() const;

    /**
     * Dump the alias database to the given output stream
     *
     * (used for debugging purposes)
     */
    void debug_dump_aliases(core::NamedFileDescriptor& out) const;

    /**
     * Dump the alias database to the given output stream
     *
     * (used for debugging purposes)
     */
    stream::SendResult debug_dump_aliases(StreamOutput& out) const;
};

}
}

#endif

