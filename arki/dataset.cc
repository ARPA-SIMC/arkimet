#include "config.h"
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/file.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/iseg.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/fromfunction.h>
#include <arki/dataset/testlarge.h>
#include <arki/dataset/reporter.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/sort.h>
#include <arki/utils.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/postprocess.h>
#include <arki/report.h>
#include <arki/summary.h>

#ifdef HAVE_LIBCURL
#include <arki/dataset/http.h>
#endif

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

DataQuery::DataQuery() : with_data(false) {}
DataQuery::DataQuery(const std::string& matcher, bool with_data) : matcher(Matcher::parse(matcher)), with_data(with_data), sorter(0) {}
DataQuery::DataQuery(const Matcher& matcher, bool with_data) : matcher(matcher), with_data(with_data), sorter(0) {}
DataQuery::~DataQuery() {}

Config::Config() {}

Config::Config(const std::string& name) : name(name) {}

Config::Config(const ConfigFile& cfg)
    : name(cfg.value("name")), cfg(cfg.values())
{
}

std::unique_ptr<Reader> Config::create_reader() const { throw std::runtime_error("reader not implemented for dataset " + name); }
std::unique_ptr<Writer> Config::create_writer() const { throw std::runtime_error("writer not implemented for dataset " + name); }
std::unique_ptr<Checker> Config::create_checker() const { throw std::runtime_error("checker not implemented for dataset " + name); }

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));

    if (type == "iseg")
        return dataset::iseg::Config::create(cfg);
    if (type == "ondisk2")
        return dataset::ondisk2::Config::create(cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Config::create(cfg);
#ifdef HAVE_LIBCURL
    if (type == "remote")
        return dataset::http::Config::create(cfg);
#endif
    if (type == "outbound")
        return outbound::Config::create(cfg);
    if (type == "discard")
        return empty::Config::create(cfg);
    if (type == "file")
        return dataset::FileConfig::create(cfg);
    if (type == "fromfunction")
        return fromfunction::Config::create(cfg);
    if (type == "testlarge")
        return testlarge::Config::create(cfg);

    throw std::runtime_error("cannot use configuration: unknown dataset type \""+type+"\"");
}


std::string Base::name() const
{
    if (m_parent)
        return m_parent->name() + "." + config().name;
    else
        return config().name;
}

void Base::set_parent(Base& p)
{
    m_parent = &p;
}


void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    query_data(DataQuery(matcher), [&](unique_ptr<Metadata> md) { summary.add(*md); return true; });
}

void Reader::query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            bool first = true;
            query_data(q, [&](unique_ptr<Metadata> md) {
                if (first)
                {
                    if (q.data_start_hook) q.data_start_hook(out);
                    first = false;
                }
                md->stream_data(out);
                return true;
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(config().cfg);
            postproc.set_data_start_hook(q.data_start_hook);
            postproc.start();
            query_data(q, [&](unique_ptr<Metadata> md) { return postproc.process(move(md)); });
            postproc.flush();
            break;
        }
        case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
            Report rep;
            rep.captureOutput(out);
            rep.load(q.param);
            query_data(q, [&](unique_ptr<Metadata> md) { return rep.eat(move(md)); });
            rep.report();
#endif
            break;
        }
        case dataset::ByteQuery::BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
            Report rep;
            rep.captureOutput(out);
            rep.load(q.param);
            Summary s;
            query_summary(q.matcher, s);
            rep(s);
            rep.report();
#endif
            break;
        }
        default:
        {
            stringstream s;
            s << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(s.str());
        }
    }
}

void Reader::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end)
{
}

#ifdef HAVE_LUA
Reader* Reader::lua_check(lua_State* L, int idx)
{
    return *(Reader**)luaL_checkudata(L, idx, "arki.rodataset");
}

void DataQuery::lua_from_table(lua_State* L, int idx)
{
    lua_pushstring(L, "matcher");
    lua_gettable(L, 2);
    matcher = Matcher::lua_check(L, -1);
    lua_pop(L, 1);

    lua_pushstring(L, "with_data");
    lua_gettable(L, 2);
    with_data = lua_toboolean(L, -1);
    lua_pop(L, 1);

    lua_pushstring(L, "sorter");
    lua_gettable(L, 2);
    const char* str_sorter = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (str_sorter)
        sorter = sort::Compare::parse(str_sorter);
    else
        sorter = 0;
}

void DataQuery::lua_push_table(lua_State* L, int idx) const
{
    string str;

    if (idx < 0) idx -= 2;

    // table["matcher"] = this->matcher
    lua_pushstring(L, "matcher");
    str = matcher.toString();
    lua_pushstring(L, str.c_str());
    lua_settable(L, idx);

    // table["with_data"] = this->with_data
    lua_pushstring(L, "with_data");
    lua_pushboolean(L, with_data);
    lua_settable(L, idx);

    // table["sorter"] = this->sorter
    lua_pushstring(L, "sorter");
    if (sorter)
    {
        str = sorter->toString();
        lua_pushstring(L, str.c_str());
    }
    else
    {
        lua_pushnil(L);
    }
    lua_settable(L, idx);
}

namespace {

// Metadata consumer that passes the metadata to a Lua function
struct LuaConsumer
{
    lua_State* L;
    int funcid;

    LuaConsumer(lua_State* L, int funcid) : L(L), funcid(funcid) {}
    ~LuaConsumer()
    {
        // Unindex the function
        luaL_unref(L, LUA_REGISTRYINDEX, funcid);
    }

    bool eat(std::unique_ptr<Metadata>&& md)
    {
        // Get the function
        lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

        // Push the metadata, handing it over to Lua's garbage collector
        Metadata::lua_push(L, move(md));

        // Call the function
        if (lua_pcall(L, 1, 1, 0))
        {
            string error = lua_tostring(L, -1);
            lua_pop(L, 1);
            throw std::runtime_error("cannot run metadata consumer function: " + error);
        }

        int res = lua_toboolean(L, -1);
        lua_pop(L, 1);
        return res;
    }

    static std::unique_ptr<LuaConsumer> lua_check(lua_State* L, int idx)
    {
        luaL_checktype(L, idx, LUA_TFUNCTION);

        // Ref the created function into the registry
        lua_pushvalue(L, idx);
        int funcid = luaL_ref(L, LUA_REGISTRYINDEX);

        // Create a consumer using the function
        return unique_ptr<LuaConsumer>(new LuaConsumer(L, funcid));
    }
};

}

static int arkilua_queryData(lua_State *L)
{
    // queryData(self, { matcher="", withdata=false, sorter="" }, consumer_func)
    Reader* rd = Reader::lua_check(L, 1);
    luaL_argcheck(L, lua_istable(L, 2), 2, "`table' expected");
    luaL_argcheck(L, lua_isfunction(L, 3), 3, "`function' expected");

    // Create a DataQuery with data from the table
    dataset::DataQuery dq;
    dq.lua_from_table(L, 2);

    // Create metadata consumer proxy
    std::unique_ptr<LuaConsumer> mdc = LuaConsumer::lua_check(L, 3);

    // Run the query
    rd->query_data(dq, [&](unique_ptr<Metadata> md) { return mdc->eat(move(md)); });

    return 0;
}

static int arkilua_query_summary(lua_State *L)
{
    // query_summary(self, matcher="", summary)
    Reader* rd = Reader::lua_check(L, 1);
    Matcher matcher = Matcher::lua_check(L, 2);
    Summary* sum = Summary::lua_check(L, 3);
    luaL_argcheck(L, sum != NULL, 3, "`arki.summary' expected");
    rd->query_summary(matcher, *sum);
    return 0;
}

static int arkilua_tostring(lua_State *L)
{
    lua_pushstring(L, "dataset");
    return 1;
}

static const struct luaL_Reg readonlydatasetlib [] = {
    { "queryData", arkilua_queryData },
    { "querySummary", arkilua_query_summary },
    { "__tostring", arkilua_tostring },
    {NULL, NULL}
};

void Reader::lua_push(lua_State* L)
{
    utils::lua::push_object_ptr(L, this, "arki.rodataset", readonlydatasetlib);
}
#endif

std::unique_ptr<Reader> Reader::create(const ConfigFile& cfg)
{
    auto config = Config::create(cfg);
    return config->create_reader();
}

void Reader::readConfig(const std::string& path, ConfigFile& cfg)
{
#ifdef HAVE_LIBCURL
    if (str::startswith(path, "http://") || str::startswith(path, "https://"))
        return dataset::http::Reader::readConfig(path, cfg);
#endif
    if (sys::isdir(path))
        return dataset::LocalReader::readConfig(path, cfg);
    else
        return dataset::File::readConfig(path, cfg);
}


void WriterBatch::set_all_error(const std::string& note)
{
    for (auto& e: *this)
    {
        e->dataset_name.clear();
        e->md.add_note(note);
        e->result = ACQ_ERROR;
    }
}


void Writer::flush() {}

Pending Writer::test_writelock() { return Pending(); }

std::unique_ptr<Writer> Writer::create(const ConfigFile& cfg)
{
    auto config = Config::create(cfg);
    return config->create_writer();
}

void Writer::test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw std::runtime_error("cannot simulate dataset acquisition: remote datasets are not writable");
    if (type == "outbound")
        return outbound::Writer::test_acquire(cfg, batch, out);
    if (type == "discard")
        return empty::Writer::test_acquire(cfg, batch, out);

    return dataset::LocalWriter::test_acquire(cfg, batch, out);
}

CheckerConfig::CheckerConfig()
    : reporter(make_shared<NullReporter>())
{
}

CheckerConfig::CheckerConfig(std::shared_ptr<dataset::Reporter> reporter, bool readonly)
    : reporter(reporter), readonly(readonly)
{
}


std::unique_ptr<Checker> Checker::create(const ConfigFile& cfg)
{
    auto config = Config::create(cfg);
    return config->create_checker();
}

void Checker::check_issue51(CheckerConfig& opts)
{
    throw std::runtime_error(name() + ": check_issue51 not implemented for this dataset");
}

}
}
