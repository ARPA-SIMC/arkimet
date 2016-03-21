#include "gridquery.h"
#include <arki/utils/lua.h>
#include <arki/utils/string.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/nag.h>
#include <arki/postprocess.h>
#include <arki/types/typeset.h>
#include <algorithm>
#ifdef HAVE_LUA
#include <arki/report.h>
#endif

// #include <iostream>

using namespace std;
using namespace arki::utils;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace dataset {

GridQuery::GridQuery(Reader& ds) : ds(ds)
{
    // Initialise with the global summary
    ds.query_summary(Matcher(), summary);
}

void GridQuery::add(const Matcher& m)
{
    if (summary.resolveMatcher(m, items) == 0)
        throw std::runtime_error("cannot resolve " + m.toString() + ": there are no data which correspond to the matcher");
}

void GridQuery::addTime(const Time& rt)
{
    vector<Time>::iterator lb =
        lower_bound(times.begin(), times.end(), rt);
	if (lb == times.end())
		times.push_back(rt);
	else if (*lb != rt)
		times.insert(lb, rt);
}

void GridQuery::addTimes(const Time& begin, const Time& end, int step)
{
    vector<Time> items = Time::generate(begin, end, step);
    for (vector<Time>::const_iterator i = items.begin(); i != items.end(); ++i)
        addTime(*i);
}

void GridQuery::addFilter(const Matcher& m)
{
	filters.push_back(m);
}

void GridQuery::consolidate()
{
	// Feed all items into mdgrid
	for (std::vector<ItemSet>::const_iterator i = items.begin();
			i != items.end(); ++i)
		mdgrid.add(*i);

	mdgrid.consolidate();

    if (times.empty())
        throw std::runtime_error("cannot consolidate GridQuery: no times have been requested");

	// Sort times
	std::sort(times.begin(), times.end());

	// Compute wantedidx
	for (std::vector<ItemSet>::const_iterator i = items.begin(); i != items.end(); ++i)
		wantedidx.push_back(mdgrid.index(*i));
	std::sort(wantedidx.begin(), wantedidx.end());

	// Create bitmap of wanted items
	todolist.resize(wantedidx.size() * times.size(), false);

    // Check that extra filters do not conflict
    set<types::Code> codes;
    codes.insert(TYPE_REFTIME);
    for (std::map<types::Code, TypeVector>::const_iterator i = mdgrid.dims.begin();
            i != mdgrid.dims.end(); ++i)
        codes.insert(i->first);
    for (vector<Matcher>::const_iterator i = filters.begin(); i != filters.end(); ++i)
    {
        if (i->empty()) continue;
        i->foreach_type([&](types::Code code, const matcher::OR&) {
            if (codes.find(code) != codes.end())
                throw std::runtime_error("cannot consolidate GridQuery: filters conflict on " + types::tag(code));
            codes.insert(code);
        });
    }
}

Matcher GridQuery::mergedQuery() const
{
	stringstream q;
	bool added = false;

    map<Code, TypeSet> byType;
    for (std::vector<ItemSet>::const_iterator i = items.begin(); i != items.end(); ++i)
        for (ItemSet::const_iterator j = i->begin(); j != i->end(); ++j)
            byType[j->first].insert(*j->second);

    for (map<Code, TypeSet>::const_iterator i = byType.begin(); i != byType.end(); ++i)
    {
        if (added) q << "; ";
        q << types::tag(i->first) + ":";
        for (TypeSet::const_iterator j = i->second.begin(); j != i->second.end(); ++j)
		{
			if (j != i->second.begin())
				q << " or ";
			q << (*j)->exactQuery();
		}
		added = true;
	}

	// Add times
	if (added) q << "; ";
	q << "reftime:>=" << times.front() << ",<=" << times.back();

	// Add extra filters
	for (vector<Matcher>::const_iterator i = filters.begin(); i != filters.end(); ++i)
	{
		if (i->empty()) continue;
		q << "; " << i->toString();
	}

	return Matcher::parse(q.str());
}

size_t GridQuery::expectedItems() const
{
	return todolist.size();
}

bool GridQuery::checkAndMark(const ItemSet& md)
{
    // Get the reftime field from md
    const reftime::Position* rt = md.get<types::reftime::Position>();
    if (!rt) return false;

	// Find the wantedidx index
	int mdidx = mdgrid.index(md);
	if (mdidx == -1) return false;
	std::vector<int>::const_iterator i =
		std::lower_bound(wantedidx.begin(), wantedidx.end(), mdidx);
	if (*i != mdidx) return false;
	mdidx = i - wantedidx.begin();

    // Find the reftime index
    std::vector<Time>::const_iterator j =
        std::lower_bound(times.begin(), times.end(), rt->time);
    if (*j != rt->time) return false;
    int rtidx = j - times.begin();

	// Find the todolist index
	int idx = rtidx * wantedidx.size() + mdidx;

	// Reject if already seen
	if (todolist[idx]) return false;

	// Mark as seen
	todolist[idx] = true;

	// Accept
	return true;
}

bool GridQuery::satisfied() const
{
	for (std::vector<bool>::const_iterator i = todolist.begin();
			i != todolist.end(); ++i)
		if (!*i) return false;
	return true;
}

static void dumpItemset(std::ostream& out, const ItemSet& is)
{
	for (ItemSet::const_iterator i = is.begin(); i != is.end(); ++i)
	{
		if (i != is.begin())
			out << "; ";
		out << types::tag(i->first) << ":" << i->second;
	}
}

void GridQuery::dump(std::ostream& out) const
{
    if (todolist.empty())
    {
        // Not consolidated
        out << "GridQuery still being built:" << endl;
        out << "  Grid dimensions so far:" << endl;
        for (std::map<types::Code, TypeVector>::const_iterator i = mdgrid.dims.begin();
                i != mdgrid.dims.end(); ++i)
            out << "    " << types::tag(i->first) << ": "
                // FIXME: this prints pointers
                << str::join(", ", i->second.begin(), i->second.end())
                << endl;
		out << "  Combinations so far:" << endl;
		for (std::vector<ItemSet>::const_iterator i = items.begin();
				i != items.end(); ++i)
		{
			out << "    ";
			dumpItemset(out, *i);
		}
        out << "  Times so far:" << endl;
        for (std::vector<Time>::const_iterator i = times.begin();
                i != times.end(); ++i)
            out << "    " << *i << endl;
	} else {
		// Consolidated
		out << "GridQuery fully built:" << endl;
		out << "  Wanted indices:" << endl;
		for (size_t i = 0; i < wantedidx.size(); ++i)
		{
            TypeVector items(mdgrid.expand(wantedidx[i]));
			if (i == 0)
			{
				// Print titles
				out << "       ";
                for (TypeVector::const_iterator j = items.begin(); j != items.end(); ++j)
                {
                    if (j != items.begin()) out << "; ";
                    out << types::tag((*j)->type_code());
                }
                out << endl;
            }
            out << "    " << (i+1) << ": " << str::join("; ", items.begin(), items.end()) << endl;
        }
		out << "  Marked so far:" << endl;
		for (size_t i = 0; i < times.size(); ++i)
		{
			out << "    " << times[i] << ": ";
			for (size_t j = 0; j < wantedidx.size(); ++j)
			{
				int idx = i * wantedidx.size() + j;
				out << (todolist[idx] ? "1" : "0");
			}
			out << endl;
		}
	}
}

#ifdef HAVE_LUA

typedef utils::lua::ManagedUD<GridQuery> GridQueryUD;

static void arkilua_getmetatable(lua_State* L);

// Make a new summary
// Memory management of the copy will be done by Lua
static int arkilua_new(lua_State* L)
{
	Reader* ds = Reader::lua_check(L, 1);
	GridQueryUD::create(L, new GridQuery(*ds), true);

	// Set the summary for the userdata
	arkilua_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

static int arkilua_gc(lua_State *L)
{
	GridQueryUD* ud = (GridQueryUD*)luaL_checkudata(L, 1, "arki.gridquery");
	if (ud != NULL && ud->collected)
		delete ud->val;
	return 0;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "gridquery");
	return 1;
}

static int arkilua_add(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	Matcher m = Matcher::lua_check(L, 2);
	gq->add(m);
	return 0;
}

static int arkilua_addtime(lua_State *L)
{
    GridQuery* gq = GridQuery::lua_check(L, 1);
    const char* timestr luaL_checkstring(L, 2);
    gq->addTime(Time::create_sql(timestr));
    return 0;
}

static int arkilua_addtimes(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	const char* tstart = luaL_checkstring(L, 2);
	const char* tend = luaL_checkstring(L, 3);
	int tstep = luaL_checkinteger(L, 4);
    gq->addTimes(Time::create_sql(tstart),
            Time::create_sql(tend),
            tstep);
    return 0;
}

static int arkilua_addfilter(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	Matcher m = Matcher::lua_check(L, 2);
	gq->addFilter(m);
	return 0;
}

static int arkilua_consolidate(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	gq->consolidate();
	return 0;
}

static int arkilua_mergedquery(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	Matcher m = gq->mergedQuery();
	m.lua_push(L);
	return 1;
}

static int arkilua_checkandmark(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	Metadata* md = Metadata::lua_check(L, 2);
	bool res = gq->checkAndMark(*md);
	lua_pushboolean(L, res);
	return 1;
}

static int arkilua_satisfied(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	bool res = gq->satisfied();
	lua_pushboolean(L, res);
	return 1;
}

static int arkilua_dump(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	stringstream out;
	gq->dump(out);
	lua_pushlstring(L, out.str().data(), out.str().size());
	return 1;
}

static int arkilua_expecteditems(lua_State *L)
{
	GridQuery* gq = GridQuery::lua_check(L, 1);
	lua_pushnumber(L, gq->expectedItems());
	return 1;
}

static const struct luaL_Reg gridqueryclasslib [] = {
	{ "new", arkilua_new },
	{ NULL, NULL }
};

static const struct luaL_Reg gridquerylib [] = {
	{ "add", arkilua_add },
	{ "addtime", arkilua_addtime },
	{ "addtimes", arkilua_addtimes },
	{ "addfilter", arkilua_addfilter },
	{ "consolidate", arkilua_consolidate },
	{ "mergedquery", arkilua_mergedquery },
	{ "checkandmark", arkilua_checkandmark },
	{ "satisfied", arkilua_satisfied },
	{ "expecteditems", arkilua_expecteditems },
	{ "dump", arkilua_dump },
	{ "__gc", arkilua_gc },
	{ "__tostring", arkilua_tostring },
	{ NULL, NULL }
};

static void arkilua_getmetatable(lua_State* L)
{
	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki.gridquery"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushvalue(L, -2);  /* pushes the metatable */
		lua_settable(L, -3);  /* metatable.__index = metatable */

        // Load normal methods
        utils::lua::add_functions(L, gridquerylib);
	}
}

void GridQuery::lua_push(lua_State* L)
{
	GridQueryUD::create(L, this, false);
	arkilua_getmetatable(L);
	lua_setmetatable(L, -2);
}

GridQuery* GridQuery::lua_check(lua_State* L, int idx)
{
	GridQueryUD* ud = (GridQueryUD*)luaL_checkudata(L, idx, "arki.gridquery");
	if (ud) return ud->val;
	return NULL;
}

void GridQuery::lua_openlib(lua_State* L)
{
    utils::lua::add_arki_global_library(L, "gridquery", gridqueryclasslib);
}

#endif

}
}
// vim:set ts=4 sw=4:
