#include "aliases.h"
#include "arki/core/cfg.h"
#include "arki/core/curl.h"
#include "arki/core/file.h"
#include "arki/stream.h"
#include "arki/utils/string.h"
#include "utils.h"
#include <string>
#include <vector>

using namespace arki::utils;

namespace arki {
namespace matcher {

Aliases::~Aliases() { reset(); }

std::shared_ptr<OR> Aliases::get(const std::string& name) const
{
    auto i = db.find(name);
    if (i == db.end())
        return nullptr;
    return i->second;
}

void Aliases::reset() { db.clear(); }

void Aliases::serialise(core::cfg::Section& cfg) const
{
    for (auto i : db)
        cfg.set(i.first, i.second->toStringValueOnly());
}

void Aliases::validate(const Aliases& other)
{
    for (const auto& i : other.db)
    {
        auto old = db.find(i.first);
        if (old == db.end())
            // Aliases with names we do not have, do not conflict
            continue;

        std::string cur = old->second->toStringExpanded();
        std::string o   = i.second->toStringExpanded();
        if (cur != o)
            throw std::runtime_error("current alias \"" + cur +
                                     "\" conflicts with new alias \"" + o +
                                     "\"");
    }
}

void Aliases::add(const MatcherType& type, const core::cfg::Section& entries)
{
    std::vector<std::pair<std::string, std::string>> aliases;
    std::vector<std::pair<std::string, std::string>> failed;
    for (const auto& i : entries)
        aliases.push_back(make_pair(str::lower(i.first), i.second));

    /*
     * Try multiple times to catch aliases referring to other aliases.
     * We continue until the number of aliases to parse stops decreasing.
     */
    for (size_t pass = 0; !aliases.empty(); ++pass)
    {
        failed.clear();
        for (const auto& alias : aliases)
        {
            std::unique_ptr<OR> val;

            // If instantiation fails, try it again later
            try
            {
                val = OR::parse(this, type, alias.second);
            }
            catch (std::exception& e)
            {
                failed.push_back(alias);
                continue;
            }

            auto j = db.find(alias.first);
            if (j == db.end())
            {
                db.emplace(alias.first, std::move(val));
            }
            else
            {
                j->second = move(val);
            }
        }
        if (!failed.empty() && failed.size() == aliases.size())
            // If no new aliases were successfully parsed, reparse one of the
            // failing ones to raise the appropriate exception
            OR::parse(nullptr, type, failed.front().second);
        aliases = failed;
    }
}

AliasDatabase::AliasDatabase() {}
AliasDatabase::AliasDatabase(const core::cfg::Sections& cfg) { add(cfg); }

void AliasDatabase::validate(const core::cfg::Sections& cfg)
{
    // Load cfg in a temporary alias database, to resolve and expand aliases,
    // and aliases that refer to aliases, and so on
    AliasDatabase temp;
    temp.add(cfg);

    for (const auto& i : temp.aliasDatabase)
    {
        auto current = aliasDatabase.find(i.first);
        if (current == aliasDatabase.end())
            // Aliases of types we do not have, do not conflict
            continue;

        current->second.validate(i.second);
    }
}

void AliasDatabase::add(const core::cfg::Sections& cfg)
{
    for (const auto& si : cfg)
    {
        matcher::MatcherType* mt = matcher::MatcherType::find(si.first);
        if (!mt)
            // Ignore unwanted sections
            continue;
        aliasDatabase[si.first].add(*mt, *si.second);
    }
}

const matcher::Aliases* AliasDatabase::get(const std::string& type) const
{
    std::map<std::string, matcher::Aliases>::const_iterator i =
        aliasDatabase.find(type);
    if (i == aliasDatabase.end())
        return 0;
    return &(i->second);
}

std::shared_ptr<core::cfg::Sections> AliasDatabase::serialise()
{
    auto res = std::make_shared<core::cfg::Sections>();
    for (const auto& i : aliasDatabase)
    {
        auto s = res->obtain(i.first);
        i.second.serialise(*s);
    }
    return res;
}

void AliasDatabase::debug_dump(core::NamedFileDescriptor& out)
{
    auto cfg = serialise()->to_string();
    out.write_all_or_retry(cfg.data(), cfg.size());
}

stream::SendResult AliasDatabase::debug_dump(StreamOutput& out)
{
    auto cfg = serialise()->to_string();
    return out.send_buffer(cfg.data(), cfg.size());
}

std::shared_ptr<core::cfg::Sections>
load_remote_alias_database(const std::string& server)
{
    core::curl::CurlEasy m_curl;
    m_curl.reset();

    core::curl::BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(server, "aliases"));
    request.perform();
    return core::cfg::Sections::parse(request.buf, server);
}

} // namespace matcher
} // namespace arki
