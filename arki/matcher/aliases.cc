#include "aliases.h"
#include "utils.h"
#include "arki/core/cfg.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <vector>
#include <string>

using namespace arki::utils;

namespace arki {
namespace matcher {


Aliases::~Aliases()
{
    reset();
}

std::shared_ptr<OR> Aliases::get(const std::string& name) const
{
    auto i = db.find(name);
    if (i == db.end())
        return nullptr;
    return i->second;
}

void Aliases::reset()
{
    db.clear();
}

void Aliases::serialise(core::cfg::Section& cfg) const
{
    for (auto i: db)
        cfg.set(i.first, i.second->toStringValueOnly());
}

void Aliases::add(const MatcherType& type, const core::cfg::Section& entries)
{
    std::vector<std::pair<std::string, std::string>> aliases;
    std::vector<std::pair<std::string, std::string>> failed;
    for (const auto& i: entries)
        aliases.push_back(make_pair(str::lower(i.first), i.second));

    /*
     * Try multiple times to catch aliases referring to other aliases.
     * We continue until the number of aliases to parse stops decreasing.
     */
    for (size_t pass = 0; !aliases.empty(); ++pass)
    {
        failed.clear();
        for (const auto& alias: aliases)
        {
            std::unique_ptr<OR> val;

            // If instantiation fails, try it again later
            try {
                val = OR::parse(this, type, alias.second);
            } catch (std::exception& e) {
                failed.push_back(alias);
                continue;
            }

            auto j = db.find(alias.first);
            if (j == db.end())
            {
                db.insert(make_pair(alias.first, move(val)));
            } else {
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


static AliasDatabase* aliasdb = 0;

AliasDatabase::AliasDatabase() {}
AliasDatabase::AliasDatabase(const core::cfg::Sections& cfg) { add(cfg); }

void AliasDatabase::add(const core::cfg::Sections& cfg)
{
    for (const auto& si: cfg)
    {
        matcher::MatcherType* mt = matcher::MatcherType::find(si.first);
        if (!mt)
            // Ignore unwanted sections
            continue;
        aliasDatabase[si.first].add(*mt, si.second);
    }
}

void AliasDatabase::addGlobal(const core::cfg::Sections& cfg)
{
    if (!matcher::aliasdb)
        matcher::aliasdb = new AliasDatabase(cfg);
    else
        matcher::aliasdb->add(cfg);
}

const matcher::Aliases* AliasDatabase::get(const std::string& type) const
{
    std::map<std::string, matcher::Aliases>::const_iterator i = aliasDatabase.find(type);
    if (i == aliasDatabase.end())
        return 0;
    return &(i->second);
}

core::cfg::Sections AliasDatabase::serialise()
{
    core::cfg::Sections res;
    for (const auto& i: aliasDatabase)
    {
        core::cfg::Section& s = res.obtain(i.first);
        i.second.serialise(s);
    }
    return res;
}

void AliasDatabase::debug_dump(core::NamedFileDescriptor& out)
{
    auto cfg = serialise();
    cfg.write(out);
}

void AliasDatabase::debug_dump(core::AbstractOutputFile& out)
{
    auto cfg = serialise();
    cfg.write(out);
}


void read_matcher_alias_database()
{
    // Otherwise the file given in the environment variable ARKI_ALIASES is tried.
    char* fromEnv = getenv("ARKI_ALIASES");
    if (fromEnv)
    {
        sys::File in(fromEnv, O_RDONLY);
        auto sections = core::cfg::Sections::parse(in);
        matcher::AliasDatabase::addGlobal(sections);
        return;
    }

#ifdef CONF_DIR
    // Else, CONF_DIR is tried.
    std::string name = std::string(CONF_DIR) + "/match-alias.conf";
    std::unique_ptr<struct stat> st = sys::stat(name);
    if (st.get())
    {
        sys::File in(name, O_RDONLY);
        auto sections = core::cfg::Sections::parse(in);
        matcher::AliasDatabase::addGlobal(sections);
        return;
    }
#endif

    // Else, nothing is loaded.
}

}
}
