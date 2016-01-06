#include "config.h"
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/file.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/segment.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/sort.h>
#include <arki/types/assigneddataset.h>
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
using namespace arki::utils;

namespace arki {
namespace dataset {

DataQuery::DataQuery() : with_data(false) {}
DataQuery::DataQuery(const Matcher& matcher, bool with_data) : matcher(matcher), with_data(with_data), sorter(0) {}
DataQuery::~DataQuery() {}

Base::Base(const std::string& name)
    : m_name(name)
{
}

Base::Base(const std::string& name, const ConfigFile& cfg)
    : m_name(name), m_cfg(cfg.values())
{
}

Base::Base(const ConfigFile& cfg)
    : m_name(cfg.value("name")), m_cfg(cfg.values())
{
}

std::string Base::name() const
{
    if (m_parent)
        return m_parent->name() + "." + m_name;
    else
        return m_name;
}

void Base::set_parent(Base& p)
{
    m_parent = &p;
}


void Writer::flush() {}

Pending Writer::test_writelock() { return Pending(); }

void Reader::query_bytes(const dataset::ByteQuery& q, int out)
{
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            const dataset::segment::OstreamWriter* writer = nullptr;
            bool first = true;
            query_data(q, [&](unique_ptr<Metadata> md) {
                if (first)
                {
                    if (q.data_start_hook) q.data_start_hook();
                    first = false;
                }
                if (!writer)
                    writer = dataset::segment::OstreamWriter::get(md->source().format);
                writer->stream(*md, out);
                return true;
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(m_cfg);
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

void Reader::expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end)
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


Reader* Reader::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));

	if (type == "ondisk2")
		return new dataset::ondisk2::Reader(cfg);
	if (type == "simple" || type == "error" || type == "duplicates")
		return new dataset::simple::Reader(cfg);
#ifdef HAVE_LIBCURL
	if (type == "remote")
		return new dataset::HTTP(cfg);
#endif
	if (type == "outbound")
		return new dataset::Empty(cfg);
	if (type == "discard")
		return new dataset::Empty(cfg);
	if (type == "file")
		return dataset::File::create(cfg);

    throw std::runtime_error("cannot create dataset reader: unknown dataset type \""+type+"\"");
}

void Reader::readConfig(const std::string& path, ConfigFile& cfg)
{
#ifdef HAVE_LIBCURL
    if (str::startswith(path, "http://") || str::startswith(path, "https://"))
        return dataset::HTTP::readConfig(path, cfg);
#endif
    if (sys::isdir(path))
        return dataset::LocalReader::readConfig(path, cfg);
    else
        return dataset::File::readConfig(path, cfg);
}

Writer* Writer::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw std::runtime_error("cannot create dataset writer: remote datasets are not writable");
    if (type == "outbound")
        return new dataset::Outbound(cfg);
    if (type == "discard")
        return new dataset::Discard(cfg);
    return dataset::LocalWriter::create(cfg);
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw std::runtime_error("cannot simulate dataset acquisition: remote datasets are not writable");
    if (type == "outbound")
        return dataset::Outbound::testAcquire(cfg, md, out);
    if (type == "discard")
        return dataset::Discard::testAcquire(cfg, md, out);

    return dataset::LocalWriter::testAcquire(cfg, md, out);
}

void FailChecker::removeAll(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("operation not possible on dataset " + name()); }
void FailChecker::repack(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("operation not possible on dataset " + name()); }
void FailChecker::check(dataset::Reporter& reporter, bool fix, bool quick) { throw std::runtime_error("operation not possible on dataset " + name()); }

Checker* Checker::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote" || type == "outbound" || type == "discard")
        return new FailChecker(cfg);
    return dataset::LocalChecker::create(cfg);
}

}
}
