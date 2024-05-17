#include "parser.h"
#include "aliases.h"
#include "utils.h"
#include "arki/core/cfg.h"
#include "arki/utils/sys.h"
#include "arki/runtime.h"
#include <cstdlib>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

Parser::Parser()
    : aliases(new AliasDatabase)
{
}

Parser::Parser(Parser&& o)
    : aliases(o.aliases)
{
    o.aliases = nullptr;
}

Parser::~Parser()
{
    delete aliases;
}

Parser& Parser::operator=(Parser&& o)
{
    if (&o == this)
        return *this;

    delete aliases;
    aliases = o.aliases;
    o.aliases = nullptr;
    return *this;
}

Matcher Parser::parse(const std::string& pattern) const
{
    return matcher::AND::parse(*aliases, pattern);
}

void Parser::load_system_aliases()
{
    // Otherwise the file given in the environment variable ARKI_ALIASES is tried.
    const Config& cfg = Config::get();
    if (cfg.file_aliases.empty())
        return;

    if (!sys::stat(cfg.file_aliases))
        return;

    sys::File in(cfg.file_aliases, O_RDONLY);
    auto sections = core::cfg::Sections::parse(in);
    aliases->add(*sections);
    return;
}

void Parser::load_aliases(const core::cfg::Sections& cfg)
{
    aliases->add(cfg);
}

void Parser::load_remote_aliases(const std::string& server_url)
{
    if (servers_seen.find(server_url) != servers_seen.end())
        return;

    auto cfg = load_remote_alias_database(server_url);

    // Verify alias conflicts, and bail out with an exception if they exist
    aliases->validate(*cfg);

    aliases->add(*cfg);
    servers_seen.emplace(server_url);
}

std::shared_ptr<core::cfg::Sections> Parser::serialise_aliases() const
{
    return aliases->serialise();
}

void Parser::debug_dump_aliases(core::NamedFileDescriptor& out) const
{
    aliases->debug_dump(out);
}

stream::SendResult Parser::debug_dump_aliases(StreamOutput& out) const
{
    return aliases->debug_dump(out);
}

}
}
