/*
 * matcher - Match metadata expressions
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/matcher.h>
#include <arki/matcher/reftime.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/configfile.h>
#include <wibble/regexp.h>
#include <wibble/string.h>
#include <memory>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;

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


std::string OR::name() const
{
	if (empty()) return string();
	return front()->name();
}

bool OR::matchItem(const Item<>& t) const
{
	if (empty()) return true;

	for (const_iterator i = begin(); i != end(); ++i)
		if ((*i)->matchItem(t))
			return true;
	return false;
}

std::string OR::toString() const
{
    if (empty()) return string();
    return front()->name() + ":" + toStringValueOnly();
}

std::string OR::toStringExpanded() const
{
    if (empty()) return string();
    return front()->name() + ":" + toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnly() const
{
    if (not unparsed.empty()) return unparsed;
    return toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnlyExpanded() const
{
    string res;
    for (const_iterator i = begin(); i != end(); ++i)
    {
        if (i != begin())
            res += " or ";
        res += (*i)->toString();
    }
    return res;
}

OR* OR::parse(const MatcherType& mt, const std::string& pattern)
{
	auto_ptr<OR> res(new OR(pattern));

	// Fetch the alias database for this type
	const Aliases* aliases = MatcherAliasDatabase::get(mt.name);

	// Split 'patterns' on /\s*or\s*/i
	Splitter splitter("[ \t]+or[ \t]+", REG_EXTENDED | REG_ICASE);

	for (Splitter::const_iterator i = splitter.begin(pattern);
			i != splitter.end(); ++i)
	{
		const OR* exprs = aliases ? aliases->get(str::tolower(*i)) : 0;
		if (exprs)
			for (OR::const_iterator j = exprs->begin(); j != exprs->end(); ++j)
				res->push_back(*j);
		else
			res->push_back(mt.parse_func(*i));
	}

	return res.release();
}


std::string AND::name() const
{
	return "matcher";
}

bool AND::matchItem(const Item<>& t) const
{
	if (empty()) return true;

	const_iterator i = find(t->serialisationCode());
	if (i == end()) return true;

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

bool AND::matchMetadata(const Metadata& md) const
{
	if (empty()) return true;

	for (const_iterator i = begin(); i != end(); ++i)
	{
		UItem<> item = md.get(i->first);
		if (!item.defined()) return false;
		if (!i->second->matchItem(item)) return false;
	}
	return true;
}

const OR* AND::get(types::Code code) const
{
	const_iterator i = find(code);
	if (i == end()) return 0;
	return i->second->upcast<OR>();
}

std::string AND::toString() const
{
	if (empty()) return string();

	std::string res;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += "; ";
		res += i->second->toString();
	}
	return res;
}

std::string AND::toStringExpanded() const
{
	if (empty()) return string();

	std::string res;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += "; ";
		res += i->second->toStringExpanded();
	}
	return res;
}

void AND::split(const std::set<types::Code>& codes, AND& with, AND& without) const
{
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (codes.find(i->first) != codes.end())
		{
			with.insert(*i);
		} else {
			without.insert(*i);
		}
	}
}

AND* AND::parse(const std::string& pattern)
{
	auto_ptr<AND> res(new AND);

	// Split on newlines or semicolons
	wibble::Tokenizer tok(pattern, "[^\n;]+", REG_EXTENDED);

	for (wibble::Tokenizer::const_iterator i = tok.begin();
			i != tok.end(); ++i)
	{
		string expr = wibble::str::trim(*i);
		if (expr.empty()) continue;
		size_t pos = expr.find(':');
		if (pos == string::npos)
			throw wibble::exception::Consistency(
				"parsing matcher subexpression",
				"subexpression '" + expr + "' does not contain a colon (':')");

		string type = str::tolower(str::trim(expr.substr(0, pos)));
		string patterns = str::trim(expr.substr(pos+1));

		MatcherType* mt = MatcherType::find(type);
		if (mt == 0)
			throw wibble::exception::Consistency(
				"parsing matcher subexpression",
				"unknown match type: '" + type + "'");

		res->insert(make_pair(mt->code, OR::parse(*mt, patterns)));
	}

	return res.release();
}

Aliases::~Aliases()
{
	reset();
}
const OR* Aliases::get(const std::string& name) const
{
	std::map< std::string, const OR* >::const_iterator i = db.find(name);
	if (i == db.end())
		return 0;
	return i->second;
}
void Aliases::reset()
{
	for (map<string, const OR*>::iterator i = db.begin();
			i != db.end(); ++i)
		if (i->second->unref())
			delete i->second;
	db.clear();
}

void Aliases::serialise(ConfigFile& cfg) const
{
	for (std::map< std::string, const OR* >::const_iterator i = db.begin();
			i != db.end(); ++i)
		cfg.setValue(i->first, i->second->toStringValueOnly());
}

void Aliases::add(const MatcherType& type, const ConfigFile& entries)
{
	vector< pair<string, string> > aliases;
	vector< pair<string, string> > failed;
	for (ConfigFile::const_iterator i = entries.begin(); i != entries.end(); ++i)
		aliases.push_back(make_pair(str::tolower(i->first), i->second));
	
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
			auto_ptr<OR> val;

			// If instantiation fails, try it again later
			try {
				val.reset(OR::parse(type, i->second));
			} catch (std::exception& e) {
				failed.push_back(*i);
				continue;
			}

			val->ref();

			map<string, const OR*>::iterator j = db.find(i->first);
			if (j == db.end())
			{
				db.insert(make_pair(i->first, val.release()));
			} else {
				if (j->second->unref())
					delete j->second;
				j->second = val.release();
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
		with = 0;
		without = 0;
	} else {
		// Create the empty matchers and assign them right away, so we sort out
		// memory management
		matcher::AND* awith = new matcher::AND;
		with = awith;
		matcher::AND* awithout = new matcher::AND;
		without = awithout;
		m_impl->split(codes, *awith, *awithout);
		if (awith->empty()) with = 0;
		if (awithout->empty()) without = 0;
	}
}

bool Matcher::date_extremes(UItem<types::Time>& begin, UItem<types::Time>& end) const
{
	const matcher::OR* reftime = 0;

	begin.clear(); end.clear();
	
	if (!m_impl)
		return false;

	reftime = m_impl->get(types::TYPE_REFTIME);

	if (!reftime)
		return false;

	for (matcher::OR::const_iterator j = reftime->begin(); j != reftime->end(); ++j)
	{
		if (j == reftime->begin())
			(*j)->upcast<matcher::MatchReftime>()->dateRange(begin, end);
		else {
			UItem<types::Time> new_begin;
			UItem<types::Time> new_end;
			(*j)->upcast<matcher::MatchReftime>()->dateRange(new_begin, new_end);
			if (begin.defined() && (!new_begin.defined() || new_begin < begin))
				begin = new_begin;
			if (end.defined() && (!new_end.defined() || end < new_end))
				end = new_end;

		}
	}
	return begin.defined() || end.defined();
}


Matcher Matcher::parse(const std::string& pattern)
{
	return matcher::AND::parse(pattern);
}

bool Matcher::operator()(const Summary& i) const
{
	if (m_impl) return i.match(*this);
	// An empty matcher always matches
	return true;
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
static void arkilua_matchermetatable(lua_State* L);

static int arkilua_new(lua_State* L)
{
	Matcher* ud = (Matcher*)lua_newuserdata(L, sizeof(Matcher));
	if (lua_gettop(L) == 0)
		new(ud) Matcher();
	else
	{
		const char* expr = lua_tostring(L, 1);
		luaL_argcheck(L, expr != NULL, 1, "`string' expected");
		new(ud) Matcher(Matcher::parse(expr));
	}

	// Set the summary for the userdata
	arkilua_matchermetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

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
	Metadata* md = Metadata::lua_check(L, 2);
	lua_pushboolean(L, m(*md));
	return 1;
}

static const struct luaL_Reg matcherclasslib [] = {
	{ "new", arkilua_new },
	{ NULL, NULL }
};

static const struct luaL_Reg matcherlib [] = {
	{ "match", arkilua_match },
	{ "expanded", arkilua_expanded },
	{ "__tostring", arkilua_tostring },
	{ "__gc", arkilua_gc },
	{ NULL, NULL }
};

// Push the arki.matcher metatable on the stack, creating it if needed
static void arkilua_matchermetatable(lua_State* L)
{
	if (luaL_newmetatable(L, "arki.matcher"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushvalue(L, -2);  /* pushes the metatable */
		lua_settable(L, -3);  /* metatable.__index = metatable */

		// Load normal methods
		luaL_register(L, NULL, matcherlib);
	}
}

void Matcher::lua_push(lua_State* L)
{
	Matcher* ud = (Matcher*)lua_newuserdata(L, sizeof(Matcher));
	new(ud) Matcher(*this);

	arkilua_matchermetatable(L);
	lua_setmetatable(L, -2);
}

void Matcher::lua_openlib(lua_State* L)
{
	luaL_register(L, "arki.matcher", matcherclasslib);
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

// vim:set ts=4 sw=4:
