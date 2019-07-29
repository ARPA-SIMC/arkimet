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

namespace {

int arkilua_metadataconsumer(lua_State *L);
int arkilua_dataset(lua_State *L);
int arkilua_tostring(lua_State *L);

const struct luaL_Reg querymacrolib[] = {
    { "dataset", arkilua_dataset },        // qm:dataset(name) -> dataset
    { "__tostring", arkilua_tostring },
    {nullptr, nullptr}
};

}


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


Base::Base(const Options& opts)
    : m_config(std::shared_ptr<const dataset::Config>(new dataset::Config(opts.macro_cfg))),
      datasets_cfg(opts.datasets_cfg)
{
}

Base::~Base()
{
}

}
}


namespace {

class LuaMacro : public arki::qmacro::Base
{
public:
    arki::Lua *L = nullptr;
    std::map<std::string, Reader*> ds_cache;
    int funcid_querydata;
    int funcid_querysummary;

    /**
     * Create a query macro read from the query macro file with the given
     * name.
     *
     * @param cfg
     *   Configuration used to instantiate datasets
     */
    LuaMacro(const std::string& source, const arki::qmacro::Options& opts)
        : Base(opts), L(new arki::Lua), funcid_querydata(-1), funcid_querysummary(-1)
    {
        arki::Summary::lua_openlib(*L);
        arki::Matcher::lua_openlib(*L);
        arki::dataset::GridQuery::lua_openlib(*L);

        // Create the Querymacro object
        arki::utils::lua::push_object_ptr(*L, this, "arki.querymacro", querymacrolib);

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
        if (luaL_dofile(*L, source.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(*L, -1);
            // Pop the error from the stack
            lua_pop(*L, 1);
            throw std::runtime_error("cannot parse " + source + ": " + error);
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

    virtual ~LuaMacro()
    {
        if (L) delete L;
        for (auto& i: ds_cache)
            delete i.second;
    }

    /**
     * Get a dataset from the querymacro dataset cache, instantiating it in
     * the cache if it is not already there
     */
    Reader* dataset(const std::string& name)
    {
        auto i = ds_cache.find(name);
        if (i == ds_cache.end())
        {
            auto dscfg = datasets_cfg.section(name);
            if (!dscfg) return 0;
            auto ds = arki::dataset::Reader::create(*dscfg);
            auto res = ds_cache.insert(make_pair(name, ds.release()));
            i = res.first;
        }
        return i->second;
    }

    bool query_data(const arki::dataset::DataQuery& q, arki::metadata_dest_func dest) override
    {
        if (funcid_querydata == -1) return true;

        // Retrieve the Lua function registered for this
        lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querydata);

        // Pass DataQuery
        lua_newtable(*L);
        q.lua_push_table(*L, -1);

        // Push consumer C closure
        shared_ptr<arki::sort::Stream> sorter;
        if (q.sorter)
        {
            sorter.reset(new arki::sort::Stream(*q.sorter, dest));
            dest = [sorter](unique_ptr<arki::Metadata> md) { return sorter->add(move(md)); };
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

    void query_summary(const arki::Matcher& matcher, arki::Summary& summary) override
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
};


LuaMacro* checkqmacro(lua_State *L, int idx = 1)
{
    void* ud = luaL_checkudata(L, idx, "arki.querymacro");
    luaL_argcheck(L, ud != NULL, idx, "`querymacro' expected");
    return *(LuaMacro**)ud;
}

int arkilua_dataset(lua_State *L)
{
    LuaMacro* rd = checkqmacro(L);
    const char* name = luaL_checkstring(L, 2);
    arki::dataset::Reader* ds = rd->dataset(name);
    if (ds)
    {
        ds->lua_push(L);
        return 1;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int arkilua_metadataconsumer(lua_State *L)
{
    arki::Metadata* md = arki::Metadata::lua_check(L, 1);
    luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

    int considx = lua_upvalueindex(1);
    arki::metadata_dest_func* cons = (arki::metadata_dest_func*)lua_touserdata(L, considx);

    // FIXME: this copy may not be needed, but before removing it we need to
    // review memory ownership for this case
    unique_ptr<arki::Metadata> copy(new arki::Metadata(*md));
    lua_pushboolean(L, (*cons)(move(copy)));
    return 1;
}

int arkilua_tostring(lua_State *L)
{
    lua_pushstring(L, "querymacro");
    return 1;
}

}

namespace arki {
namespace qmacro {

std::vector<std::pair<std::string, std::function<std::shared_ptr<dataset::Reader>(const std::string& source, const Options& opts)>>> parsers;

std::shared_ptr<dataset::Reader> get(const Options& opts)
{
    for (const auto& entry: parsers)
    {
        std::string fname = Config::get().dir_qmacro.find_file_noerror(opts.macro_name + "." + entry.first);
        if (!fname.empty())
        {
            return entry.second(fname, opts);
        }
    }
    throw std::runtime_error("querymacro source not found for macro" + opts.macro_name);
}

void register_parser(
        const std::string& ext,
        std::function<std::shared_ptr<dataset::Reader>(const std::string& source, const Options& opts)> parser)
{
    for (const auto& entry: parsers)
        if (entry.first == ext)
            throw std::runtime_error("quarymacro parser for ." + ext + " files has already been registered");

    parsers.emplace_back(make_pair(ext, parser));
}

void init()
{
    register_parser("lua", [](const std::string& source, const Options& opts) {
        return make_shared<LuaMacro>(source, opts);
    });
}

}
}
