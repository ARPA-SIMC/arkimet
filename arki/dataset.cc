#include "config.h"
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/file.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/data.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/sort.h>
#include <arki/types/assigneddataset.h>
#include <arki/utils.h>
#include <arki/utils/dataset.h>
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

}

WritableDataset::WritableDataset()
{
}

WritableDataset::~WritableDataset()
{
}

void WritableDataset::flush() {}

Pending WritableDataset::test_writelock() { return Pending(); }

void ReadonlyDataset::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
#warning temporary implementation: make this purely abstract and locally implement the one with the eater instead
    struct ToDest : public metadata::Eater
    {
        std::function<bool(unique_ptr<Metadata>)> dest;
        ToDest(std::function<bool(unique_ptr<Metadata>)> dest) : dest(dest) {}

        bool eat(std::unique_ptr<Metadata>&& md) override
        {
            return dest(move(md));
        }
    } eater(dest);
    queryData(q, eater);
}

void ReadonlyDataset::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            ds::DataOnly dataonly(out);
            ds::DataStartHookRunner dshr(dataonly, q.data_start_hook);
            query_data(q, [&](unique_ptr<Metadata> md) { return dshr.eat(move(md)); });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(cfg);
            postproc.set_data_start_hook(q.data_start_hook);
            postproc.start();
            query_data(q, [&](unique_ptr<Metadata> md) {
                return postproc.eat(move(md));
            });
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
            querySummary(q.matcher, s);
            rep(s);
            rep.report();
#endif
            break;
        }
        default:
        {
            stringstream s;
            s << "unsupported query type: " << (int)q.type;
            throw wibble::exception::Consistency("querying dataset", s.str());
        }
    }
}

void ReadonlyDataset::query_bytes(const dataset::ByteQuery& q, int out)
{
    using namespace arki::utils;

    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            const dataset::data::OstreamWriter* writer = nullptr;
            bool first = true;
            query_data(q, [&](unique_ptr<Metadata> md) {
                if (first)
                {
                    (*q.data_start_hook)();
                    first = false;
                }
                if (!writer)
                    writer = dataset::data::OstreamWriter::get(md->source().format);
                writer->stream(*md, out);
                return true;
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(cfg);
            postproc.set_data_start_hook(q.data_start_hook);
            postproc.start();
            query_data(q, [&](unique_ptr<Metadata> md) { return postproc.eat(move(md)); });
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
            querySummary(q.matcher, s);
            rep(s);
            rep.report();
#endif
            break;
        }
        default:
        {
            stringstream s;
            s << "unsupported query type: " << (int)q.type;
            throw wibble::exception::Consistency("querying dataset", s.str());
        }
    }
}

#ifdef HAVE_LUA
ReadonlyDataset* ReadonlyDataset::lua_check(lua_State* L, int idx)
{
	return *(ReadonlyDataset**)luaL_checkudata(L, idx, "arki.rodataset");
}

namespace dataset {
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

}


static int arkilua_queryData(lua_State *L)
{
	// queryData(self, { matcher="", withdata=false, sorter="" }, consumer_func)
	ReadonlyDataset* rd = ReadonlyDataset::lua_check(L, 1);
	luaL_argcheck(L, lua_istable(L, 2), 2, "`table' expected");
	luaL_argcheck(L, lua_isfunction(L, 3), 3, "`function' expected");

	// Create a DataQuery with data from the table
	dataset::DataQuery dq;
	dq.lua_from_table(L, 2);

	// Create metadata consumer proxy
	std::unique_ptr<metadata::LuaConsumer> mdc = metadata::LuaConsumer::lua_check(L, 3);

    // Run the query
    rd->query_data(dq, [&](unique_ptr<Metadata> md) { return mdc->eat(move(md)); });

    return 0;
}

static int arkilua_querySummary(lua_State *L)
{
	// querySummary(self, matcher="", summary)
	ReadonlyDataset* rd = ReadonlyDataset::lua_check(L, 1);
	Matcher matcher = Matcher::lua_check(L, 2);
	Summary* sum = Summary::lua_check(L, 3);
	luaL_argcheck(L, sum != NULL, 3, "`arki.summary' expected");
	rd->querySummary(matcher, *sum);
	return 0;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "dataset");
	return 1;
}

static const struct luaL_Reg readonlydatasetlib [] = {
	{ "queryData", arkilua_queryData },
	{ "querySummary", arkilua_querySummary },
	{ "__tostring", arkilua_tostring },
	{NULL, NULL}
};

void ReadonlyDataset::lua_push(lua_State* L)
{
    utils::lua::push_object_ptr(L, this, "arki.rodataset", readonlydatasetlib);
}
#endif


ReadonlyDataset* ReadonlyDataset::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

	if (type == "ondisk2" || type == "test")
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

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

void ReadonlyDataset::readConfig(const std::string& path, ConfigFile& cfg)
{
#ifdef HAVE_LIBCURL
    if (str::startswith(path, "http://") || str::startswith(path, "https://"))
        return dataset::HTTP::readConfig(path, cfg);
#endif
    if (sys::isdir(path))
        return dataset::Local::readConfig(path, cfg);
    else
        return dataset::File::readConfig(path, cfg);
}

WritableDataset* WritableDataset::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw wibble::exception::Consistency("creating a dataset", "remote datasets are not writable");
	if (type == "outbound")
		return new dataset::Outbound(cfg);
	if (type == "discard")
		return new dataset::Discard(cfg);
    /*
    // TODO: create remote ones once implemented
    */
    return dataset::WritableLocal::create(cfg);
}

WritableDataset::AcquireResult WritableDataset::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw wibble::exception::Consistency("simulating dataset acquisition", "remote datasets are not writable");
	if (type == "outbound")
		return dataset::Outbound::testAcquire(cfg, md, out);
	if (type == "discard")
		return dataset::Discard::testAcquire(cfg, md, out);

    return dataset::WritableLocal::testAcquire(cfg, md, out);
}

}
