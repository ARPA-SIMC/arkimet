#include "matcher.h"
#include "matcher/reftime.h"
#include "metadata.h"
#include "configfile.h"
#include "utils/string.h"
#include "utils/lua.h"
#include "utils/regexp.h"
#include "utils/string.h"
#include <memory>
#include <cassert>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {

namespace matcher {

// Registry of matcher information
static std::map<std::string, MatcherType*>* matchers = 0;

MatcherType::MatcherType(const std::string& name, types::Code code, subexpr_parser parse_func)
    : name(name), code(code), parse_func(parse_func)
{
	// Ensure that the map is created before we add items to it
	if (!matchers)
	{
		matchers = new map<string, MatcherType*>;
	}

	(*matchers)[name] = this;
}

MatcherType::~MatcherType()
{
	if (!matchers)
		return;
	
	matchers->erase(name);
}

MatcherType* MatcherType::find(const std::string& name)
{
	if (!matchers) return 0;
	std::map<std::string, MatcherType*>::const_iterator i = matchers->find(name);
	if (i == matchers->end()) return 0;
	return i->second;
}

std::vector<std::string> MatcherType::matcherNames()
{
	vector<string> res;
	for (std::map<std::string, MatcherType*>::const_iterator i = matchers->begin();
			i != matchers->end(); ++i)
		res.push_back(i->first);
	return res;
}

static MatcherAliasDatabase* aliasdb = 0;

OR::~OR() {}

std::string OR::name() const
{
    if (components.empty()) return string();
    return components.front()->name();
}

bool OR::matchItem(const Type& t) const
{
    if (components.empty()) return true;

    for (auto i: components)
        if (i->matchItem(t))
            return true;
    return false;
}

std::string OR::toString() const
{
    if (components.empty()) return string();
    return components.front()->name() + ":" + toStringValueOnly();
}

std::string OR::toStringExpanded() const
{
    if (components.empty()) return string();
    return components.front()->name() + ":" + toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnly() const
{
    if (not unparsed.empty()) return unparsed;
    return toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnlyExpanded() const
{
    string res;
    for (auto i: components)
    {
        if (!res.empty()) res += " or ";
        res += i->toString();
    }
    return res;
}

bool OR::restrict_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const
{
    for (auto i: components)
    {
        const matcher::MatchReftime* rt = dynamic_cast<const matcher::MatchReftime*>(i.get());
        assert(rt != nullptr);
        if (!rt->restrict_date_range(begin, end))
            return false;
    }
    return true;
}

std::string OR::toReftimeSQL(const std::string& column) const
{
    if (components.size() == 1)
    {
        const matcher::MatchReftime* mr = dynamic_cast<const matcher::MatchReftime*>(components[0].get());
        return mr->sql(column);
    } else {
        string res = "(";
        bool first = true;

        for (const auto& i: components)
        {
            const matcher::MatchReftime* mr = dynamic_cast<const matcher::MatchReftime*>(i.get());
            if (!mr) throw std::runtime_error("arkimet bug: toReftimeSQL called on non-reftime matchers");
            if (first)
                first = false;
            else
                res += " OR ";
            res += mr->sql(column);
        }

        res += ")";
        return res;
    }
}

unique_ptr<OR> OR::parse(const MatcherType& mt, const std::string& pattern)
{
    unique_ptr<OR> res(new OR(pattern));

    // Fetch the alias database for this type
    const Aliases* aliases = MatcherAliasDatabase::get(mt.name);

    // Split 'patterns' on /\s*or\s*/i
    Splitter splitter("[ \t]+or[ \t]+", REG_EXTENDED | REG_ICASE);

    for (Splitter::const_iterator i = splitter.begin(pattern); i != splitter.end(); ++i)
    {
        shared_ptr<OR> exprs = aliases ? aliases->get(str::lower(*i)) : nullptr;
        if (exprs)
            for (auto j: exprs->components)
                res->components.push_back(j);
        else
            res->components.push_back(mt.parse_func(*i));
    }

    return res;
}

AND::~AND() {}

std::string AND::name() const
{
    return "matcher";
}

bool AND::matchItem(const Type& t) const
{
    if (empty()) return true;

    auto i = components.find(t.type_code());
    if (i == components.end()) return true;

    return i->second->matchItem(t);
}

template<typename COLL>
static bool mdmatch(const Implementation& matcher, const COLL& c)
{
	for (typename COLL::const_iterator i = c.begin(); i != c.end(); ++i)
		if (matcher.matchItem(*i))
			return true;
	return false;
}

bool AND::matchItemSet(const ItemSet& md) const
{
    if (empty()) return true;

    for (const auto& i: components)
    {
        if (!i.second) return false;
        const Type* item = md.get(i.first);
        if (!item) return false;
        if (!i.second->matchItem(*item)) return false;
    }
    return true;
}

shared_ptr<OR> AND::get(types::Code code) const
{
    auto i = components.find(code);
    if (i == components.end()) return nullptr;
    return i->second;
}

void AND::foreach_type(std::function<void(types::Code, const OR&)> dest) const
{
    for (const auto& i: components)
        dest(i.first, *i.second);
}

std::string AND::toString() const
{
    if (components.empty()) return string();

    std::string res;
    for (const auto& i: components)
    {
        if (!res.empty()) res += "; ";
        res += i.second->toString();
    }
    return res;
}

std::string AND::toStringExpanded() const
{
    if (components.empty()) return string();

    std::string res;
    for (const auto& i: components)
    {
        if (!res.empty()) res += "; ";
        res += i.second->toStringExpanded();
    }
    return res;
}

void AND::split(const std::set<types::Code>& codes, AND& with, AND& without) const
{
    for (const auto& i: components)
    {
        if (codes.find(i.first) != codes.end())
        {
            with.components.insert(i);
        } else {
            without.components.insert(i);
        }
    }
}

unique_ptr<AND> AND::parse(const std::string& pattern)
{
    unique_ptr<AND> res(new AND);

    // Split on newlines or semicolons
    Tokenizer tok(pattern, "[^\n;]+", REG_EXTENDED);

    for (Tokenizer::const_iterator i = tok.begin(); i != tok.end(); ++i)
    {
        string expr = str::strip(*i);
        if (expr.empty()) continue;
        size_t pos = expr.find(':');
        if (pos == string::npos)
            throw std::runtime_error("cannot parse matcher subexpression '" + expr + "' does not contain a colon (':')");

        string type = str::lower(str::strip(expr.substr(0, pos)));
        string patterns = str::strip(expr.substr(pos+1));

        MatcherType* mt = MatcherType::find(type);
        if (mt == 0)
            throw std::runtime_error("cannot parse matcher subexpression: unknown match type: '" + type + "'");

        res->components.insert(make_pair(mt->code, OR::parse(*mt, patterns)));
    }

    return res;
}

Aliases::~Aliases()
{
	reset();
}

shared_ptr<OR> Aliases::get(const std::string& name) const
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

void Aliases::serialise(ConfigFile& cfg) const
{
    for (auto i: db)
        cfg.setValue(i.first, i.second->toStringValueOnly());
}

void Aliases::add(const MatcherType& type, const ConfigFile& entries)
{
    vector< pair<string, string> > aliases;
    vector< pair<string, string> > failed;
    for (ConfigFile::const_iterator i = entries.begin(); i != entries.end(); ++i)
        aliases.push_back(make_pair(str::lower(i->first), i->second));

	/*
	 * Try multiple times to catch aliases referring to other aliases.
	 * We continue until the number of aliases to parse stops decreasing.
     */
    for (size_t pass = 0; !aliases.empty(); ++pass)
	{
		failed.clear();
		for (vector< pair<string, string> >::const_iterator i = aliases.begin();
				i != aliases.end(); ++i)
		{
			unique_ptr<OR> val;

            // If instantiation fails, try it again later
            try {
                val = OR::parse(type, i->second);
            } catch (std::exception& e) {
                failed.push_back(*i);
                continue;
            }

            auto j = db.find(i->first);
            if (j == db.end())
            {
                db.insert(make_pair(i->first, move(val)));
            } else {
                j->second = move(val);
            }
        }
		if (!failed.empty() && failed.size() == aliases.size())
			// If no new aliases were successfully parsed, reparse one of the
			// failing ones to raise the appropriate exception
			OR::parse(type, failed.front().second);
		aliases = failed;
	}
}

}

void Matcher::split(const std::set<types::Code>& codes, Matcher& with, Matcher& without) const
{
    if (!m_impl)
    {
        with = Matcher();
        without = Matcher();
    } else {
        // Create the empty matchers and assign them right away, so we sort out
        // memory management
        unique_ptr<matcher::AND> awith(new matcher::AND);
        unique_ptr<matcher::AND> awithout(new matcher::AND);

        m_impl->split(codes, *awith, *awithout);

        if (awith->empty())
            with = Matcher();
        else
            with = Matcher(move(awith));


        if (awithout->empty())
            without = Matcher();
        else
            without = Matcher(move(awithout));
    }
}

bool Matcher::restrict_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end) const
{
    shared_ptr<matcher::OR> reftime;

    // We have nothing to match: we match the open range
    if (!m_impl) return true;

    reftime = m_impl->get(TYPE_REFTIME);

    // We have no reftime to match: we match the open range
    if (!reftime) return true;

    if (!reftime->restrict_date_range(begin, end))
        return false;

    return true;
}


Matcher Matcher::parse(const std::string& pattern)
{
	return matcher::AND::parse(pattern);
}

std::ostream& operator<<(std::ostream& o, const Matcher& m)
{
	return o << m.toString();
}

#if 0
bool Matcher::match(const Metadata& md) const
{
	if (!origin.match(md.origins)) return false;
	if (!product.match(md.products)) return false;
	if (!level.match(md.levels)) return false;
	if (!timerange.match(md.timeranges)) return false;
	if (!area.match(md.areas)) return false;
	if (!proddef.match(md.proddef)) return false;
	if (!reftime.match(md.reftime)) return false;
	return true;
}

static inline void appendList(std::string& str, char sep, const std::string& trail)
{
	if (!str.empty())
		str += sep;
	str += trail;
}

std::string Matcher::toString(bool formatted) const
{
	string res;
	char sep = formatted ? '\n' : ';';

	if (!origin.empty()) appendList(res, sep, origin.toString());
	if (!product.empty()) appendList(res, sep, product.toString());
	if (!level.empty()) appendList(res, sep, level.toString());
	if (!timerange.empty()) appendList(res, sep, timerange.toString());
	if (!area.empty()) appendList(res, sep, area.toString());
	if (!proddef.empty()) appendList(res, sep, proddef.toString());
	if (!reftime.empty()) appendList(res, sep, reftime.toString());

	return res;
}
#endif

void Matcher::register_matcher(const std::string& name, types::Code code, matcher::MatcherType::subexpr_parser parse_func)
{
    // TODO: when we are done getting rid of static constructors, reorganize
    // the code not to need this awkward new
    new matcher::MatcherType(name, code, parse_func);
}

#ifdef HAVE_LUA

static int arkilua_gc (lua_State *L)
{
	Matcher* ud = (Matcher*)luaL_checkudata(L, 1, "arki.matcher");
	ud->~Matcher();
	return 0;
}

static int arkilua_tostring (lua_State *L)
{
	Matcher m = Matcher::lua_check(L, 1);
	lua_pushstring(L, m.toString().c_str());
	return 1;
}

static int arkilua_expanded (lua_State *L)
{
	Matcher m = Matcher::lua_check(L, 1);
	lua_pushstring(L, m.toStringExpanded().c_str());
	return 1;
}

static int arkilua_match (lua_State *L)
{
    Matcher m = Matcher::lua_check(L, 1);
    ItemSet* md = Metadata::lua_check(L, 2);
    lua_pushboolean(L, m(*md));
    return 1;
}

static const struct luaL_Reg matcherlib [] = {
    { "match", arkilua_match },
    { "expanded", arkilua_expanded },
    { "__tostring", arkilua_tostring },
    { "__gc", arkilua_gc },
    { NULL, NULL }
};

static int arkilua_new(lua_State* L)
{
    if (lua_gettop(L) == 0)
        utils::lua::push_object_copy(L, Matcher(), "arki.matcher", matcherlib);
    else
    {
        const char* expr = lua_tostring(L, 1);
        luaL_argcheck(L, expr != NULL, 1, "`string' expected");
        utils::lua::push_object_copy(L, Matcher::parse(expr), "arki.matcher", matcherlib);
    }

    return 1;
}

static const struct luaL_Reg matcherclasslib [] = {
	{ "new", arkilua_new },
	{ NULL, NULL }
};

void Matcher::lua_push(lua_State* L)
{
    utils::lua::push_object_copy(L, *this, "arki.matcher", matcherlib);
}

void Matcher::lua_openlib(lua_State* L)
{
    utils::lua::add_arki_global_library(L, "matcher", matcherclasslib);
}

Matcher Matcher::lua_check(lua_State* L, int idx)
{
	if (lua_isstring(L, idx))
	{
		return Matcher::parse(lua_tostring(L, idx));
	} else {
		Matcher* ud = (Matcher*)luaL_checkudata(L, idx, "arki.matcher");
		return *ud;
	}
}
#endif

MatcherAliasDatabase::MatcherAliasDatabase() {}
MatcherAliasDatabase::MatcherAliasDatabase(const ConfigFile& cfg) { add(cfg); }

void MatcherAliasDatabase::add(const ConfigFile& cfg)
{
	for (ConfigFile::const_section_iterator sec = cfg.sectionBegin();
			sec != cfg.sectionEnd(); ++sec)
	{
		matcher::MatcherType* mt = matcher::MatcherType::find(sec->first);
		if (!mt)
			// Ignore unwanted sections
			continue;
		aliasDatabase[sec->first].add(*mt, *sec->second);
	}
}

void MatcherAliasDatabase::addGlobal(const ConfigFile& cfg)
{
	if (!matcher::aliasdb)
		matcher::aliasdb = new MatcherAliasDatabase(cfg);
	else
		matcher::aliasdb->add(cfg);
}

const matcher::Aliases* MatcherAliasDatabase::get(const std::string& type)
{
	if (!matcher::aliasdb)
		return 0;

	const std::map<std::string, matcher::Aliases>& aliasDatabase = matcher::aliasdb->aliasDatabase;
	std::map<std::string, matcher::Aliases>::const_iterator i = aliasDatabase.find(type);
	if (i == aliasDatabase.end())
		return 0;
	return &(i->second);
}

const void MatcherAliasDatabase::reset()
{
	if (matcher::aliasdb)
		matcher::aliasdb->aliasDatabase.clear();
}

void MatcherAliasDatabase::serialise(ConfigFile& cfg)
{
	if (!matcher::aliasdb)
		return;

	for (std::map<std::string, matcher::Aliases>::const_iterator i = matcher::aliasdb->aliasDatabase.begin();
			i != matcher::aliasdb->aliasDatabase.end(); ++i)
	{
		ConfigFile* c = cfg.obtainSection(i->first);
		i->second.serialise(*c);
	}
}

void MatcherAliasDatabase::debug_dump(std::ostream& out)
{
    ConfigFile cfg;
    serialise(cfg);
    cfg.output(out, "(debug output)");
}

}
