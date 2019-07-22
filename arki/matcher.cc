#include "matcher.h"
#include "matcher/aliases.h"
#include "matcher/utils.h"
#include "core/cfg.h"
#include "metadata.h"
#include "utils/string.h"
#include "utils/lua.h"
#include "utils/string.h"
#include <memory>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {

Matcher::Matcher(std::unique_ptr<matcher::AND>&& impl) : m_impl(move(impl)) {}
Matcher::Matcher(std::shared_ptr<matcher::AND> impl) : m_impl(impl) {}

bool Matcher::empty() const { return m_impl.get() == 0 || m_impl->empty(); }

const matcher::AND* Matcher::operator->() const
{
    if (!m_impl.get())
        throw std::runtime_error("cannot access matcher: matcher is empty");
    return m_impl.get();
}

std::string Matcher::name() const
{
    if (m_impl.get()) return m_impl->name();
    return std::string();
}

bool Matcher::operator()(const types::Type& t) const
{
    if (m_impl.get()) return m_impl->matchItem(t);
    // An empty matcher always matches
    return true;
}

bool Matcher::operator()(const types::ItemSet& md) const
{
    if (m_impl.get()) return m_impl->matchItemSet(md);
    // An empty matcher always matches
    return true;
}

std::shared_ptr<matcher::OR> Matcher::get(types::Code code) const
{
    if (m_impl) return m_impl->get(code);
    return nullptr;
}

void Matcher::foreach_type(std::function<void(types::Code, const matcher::OR&)> dest) const
{
    if (!m_impl) return;
    return m_impl->foreach_type(dest);
}

std::string Matcher::toString() const
{
    if (m_impl) return m_impl->toString();
    return std::string();
}

std::string Matcher::toStringExpanded() const
{
    if (m_impl) return m_impl->toStringExpanded();
    return std::string();
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

}
