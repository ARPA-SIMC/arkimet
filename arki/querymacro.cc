#include "config.h"
#include "querymacro.h"
#include "metadata.h"
#include "metadata/consumer.h"
#include "summary.h"
#include "dataset/gridquery.h"
#include "runtime.h"
#include "utils/lua.h"
#include "sort.h"
#include "utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace qmacro {

Options::Options(const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query)
    : datasets_cfg(datasets_cfg), query(query)
{
    size_t pos = name.find(" ");
    if (pos == string::npos)
    {
        macro_name = name;
    } else {
        macro_name = name.substr(0, pos);
        macro_args = str::strip(name.substr(pos + 1));
    }

}

Options::Options(const core::cfg::Section& macro_cfg, const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query)
    : macro_cfg(macro_cfg), datasets_cfg(datasets_cfg), query(query)
{
    size_t pos = name.find(" ");
    if (pos == string::npos)
    {
        macro_name = name;
    } else {
        macro_name = name.substr(0, pos);
        macro_args = str::strip(name.substr(pos + 1));
    }

}

namespace {

class LuaMacro : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;
    std::map<std::string, Reader*> ds_cache;

public:
    core::cfg::Sections datasets_cfg;
    Lua *L = nullptr;
    int funcid_querydata;
    int funcid_querysummary;

    /**
     * Create a query macro read from the query macro file with the given
     * name.
     *
     * @param cfg
     *   Configuration used to instantiate datasets
     */
    LuaMacro(const Options& opts);
    virtual ~LuaMacro()
    {
        if (L) delete L;
        for (std::map<std::string, Reader*>::iterator i = ds_cache.begin();
                i != ds_cache.end(); ++i)
            delete i->second;
    }

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "querymacro"; }

    /**
     * Get a dataset from the querymacro dataset cache, instantiating it in
     * the cache if it is not already there
     */
    Reader* dataset(const std::string& name);

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};


static LuaMacro* checkqmacro(lua_State *L, int idx = 1)
{
    void* ud = luaL_checkudata(L, idx, "arki.querymacro");
    luaL_argcheck(L, ud != NULL, idx, "`querymacro' expected");
    return *(LuaMacro**)ud;
}

static int arkilua_dataset(lua_State *L)
{
    LuaMacro* rd = checkqmacro(L);
    const char* name = luaL_checkstring(L, 2);
    dataset::Reader* ds = rd->dataset(name);
    if (ds)
    {
        ds->lua_push(L);
        return 1;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

static int arkilua_metadataconsumer(lua_State *L)
{
    Metadata* md = Metadata::lua_check(L, 1);
    luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

    int considx = lua_upvalueindex(1);
    metadata_dest_func* cons = (metadata_dest_func*)lua_touserdata(L, considx);

    // FIXME: this copy may not be needed, but before removing it we need to
    // review memory ownership for this case
    unique_ptr<Metadata> copy(new Metadata(*md));
    lua_pushboolean(L, (*cons)(move(copy)));
    return 1;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "querymacro");
	return 1;
}

static const struct luaL_Reg querymacrolib[] = {
	{ "dataset", arkilua_dataset },	                // qm:dataset(name) -> dataset
	{ "__tostring", arkilua_tostring },
	{NULL, NULL}
};


LuaMacro::LuaMacro(const Options& opts)
    : datasets_cfg(opts.datasets_cfg), L(new Lua), funcid_querydata(-1), funcid_querysummary(-1)
{
    m_config = std::shared_ptr<const dataset::Config>(new dataset::Config(opts.macro_cfg));
    Summary::lua_openlib(*L);
    Matcher::lua_openlib(*L);
    dataset::GridQuery::lua_openlib(*L);

    // Create the Querymacro object
    utils::lua::push_object_ptr(*L, this, "arki.querymacro", querymacrolib);

	// global qmacro = our userdata object
	lua_setglobal(*L, "qmacro");

    // Load the data as a global variable
    lua_pushstring(*L, opts.query.c_str());
    lua_setglobal(*L, "query");

    if (opts.macro_args.empty())
    {
        lua_pushnil(*L);
    } else {
        lua_pushstring(*L, opts.macro_args.c_str());
    }
    // Load the arguments as a global variable
    lua_setglobal(*L, "args");

    /// Load the right qmacro file
    string fname = Config::get().dir_qmacro.find_file(opts.macro_name + ".lua");
    if (luaL_dofile(*L, fname.c_str()))
    {
        // Copy the error, so that it will exist after the pop
        string error = lua_tostring(*L, -1);
        // Pop the error from the stack
        lua_pop(*L, 1);
        throw std::runtime_error("cannot parse " + fname + ": " + error);
    }

	/// Index the queryData function
	lua_getglobal(*L, "queryData");
	if (lua_isfunction(*L, -1))
		funcid_querydata = luaL_ref(*L, LUA_REGISTRYINDEX);

	/// Index the querySummary function
	lua_getglobal(*L, "querySummary");
	if (lua_isfunction(*L, -1))
		funcid_querysummary = luaL_ref(*L, LUA_REGISTRYINDEX);

	// utils::lua::dumpstack(*L, "Afterinit", cerr);
}

dataset::Reader* LuaMacro::dataset(const std::string& name)
{
    std::map<std::string, dataset::Reader*>::iterator i = ds_cache.find(name);
    if (i == ds_cache.end())
    {
        const core::cfg::Section* dscfg = datasets_cfg.section(name);
        if (!dscfg) return 0;
        auto ds = dataset::Reader::create(*dscfg);
        pair<map<string, dataset::Reader*>::iterator, bool> res = ds_cache.insert(make_pair(name, ds.release()));
        i = res.first;
    }
    return i->second;
}

bool LuaMacro::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (funcid_querydata == -1) return true;

    // Retrieve the Lua function registered for this
    lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querydata);

    // Pass DataQuery
    lua_newtable(*L);
    q.lua_push_table(*L, -1);

    // Push consumer C closure
    shared_ptr<sort::Stream> sorter;
    if (q.sorter)
    {
        sorter.reset(new sort::Stream(*q.sorter, dest));
        dest = [sorter](unique_ptr<Metadata> md) { return sorter->add(move(md)); };
    }

    lua_pushlightuserdata(*L, &dest);
    lua_pushcclosure(*L, arkilua_metadataconsumer, 1);

    // Call the function
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot run queryData function: " + error);
    }

    if (sorter) return sorter->flush();
    return true;
}

void LuaMacro::query_summary(const Matcher& matcher, Summary& summary)
{
	if (funcid_querysummary == -1) return;

	// Retrieve the Lua function registered for this
	lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querysummary);

	// Pass matcher
	string m = matcher.toString();
	lua_pushstring(*L, m.c_str());

	// Pass summary
	summary.lua_push(*L);

    // Call the function
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot run querySummary function: " + error);
    }
}

}

std::shared_ptr<dataset::Reader> get(const Options& opts)
{
    return make_shared<LuaMacro>(opts);
}

}
}
