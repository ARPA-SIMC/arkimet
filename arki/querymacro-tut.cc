#include <arki/types/tests.h>
#include <arki/querymacro.h>
#include <arki/runtime/config.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/summary.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/sort.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/lua.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::utils;
using namespace arki::types;
using arki::core::Time;

struct arki_querymacro_shar {
    ConfigFile dispatch_cfg;

	arki_querymacro_shar()
	{
		runtime::readMatcherAliasDatabase();

		// Cleanup the test datasets
		system("rm -rf testds");
		system("mkdir testds");

        // In-memory dataset configuration
        string conf =
            "[testds]\n"
            "unique = origin\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "name = testds\n"
            "path = testds\n";
        dispatch_cfg.parse(conf, "(memory)");

        // Import all data from test.grib1
        Metadata md;
        scan::Grib scanner;
        scanner.test_open("inbound/test.grib1");

        auto config = dataset::ondisk2::Config::create(*dispatch_cfg.section("testds"));
        dataset::ondisk2::Writer testds(config);
        vector<Time> times;
        times.push_back(Time(2009, 8, 7, 0, 0, 0));
        times.push_back(Time(2009, 8, 8, 0, 0, 0));
        times.push_back(Time(2009, 8, 9, 0, 0, 0));
        size_t count = 0;
        while (scanner.next(md))
        {
            for (const auto& i: times)
            {
                md.set(Reftime::createPosition(i));
                ensure(testds.acquire(md) == dataset::Writer::ACQ_OK);
            }
            ++count;
        }
		ensure_equals(count, 3u);
		testds.flush();
	}
};
TESTGRP(arki_querymacro);

// Test running queries from Lua
def_test(1)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "test0", "foo");

	lua_getglobal(*qm.L, "count1");
	int count1 = lua_tointeger(*qm.L, -1);

	lua_getglobal(*qm.L, "count2");
	int count2 = lua_tointeger(*qm.L, -1);

	lua_pop(*qm.L, 2);

	ensure_equals(count1, 9);
	ensure_equals(count2, 3);
}

// Lua script that simply passes through the queries
def_test(2)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "noop", "testds");

    metadata::Collection mdc(qm, Matcher());
    ensure_equals(mdc.size(), 9u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());
    ensure(mdc[2].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 9u);
}

// Lua script that simply passes through the queries, making temporary copies of data
def_test(3)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "noopcopy", "testds");

    metadata::Collection mdc(qm, Matcher());
    ensure_equals(mdc.size(), 9u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());
    ensure(mdc[2].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 9u);
}

// Try "expa" matchers
def_test(4)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "expa", 
            "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
            "ds:testds. d:2009-08-07. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n"
    //          utils::readFile("misc/erse00.expa")
    );

    metadata::Collection mdc(qm, Matcher());
    ensure_equals(mdc.size(), 2u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 2u);
}

// Try "expa" matchers with parameter
def_test(5)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "expa 2009-08-08", 
            "ds:testds. d:@. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
            "ds:testds. d:@-1. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n"
//          utils::readFile("misc/erse00.expa")
    );

    metadata::Collection mdc(qm, Matcher());
    ensure_equals(mdc.size(), 2u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 2u);
}


// Try "gridspace" matchers
def_test(6)
{
    ConfigFile cfg;
    {
        Querymacro qm(cfg, dispatch_cfg, "gridspace", 
                "dataset: testds\n"
                "addtime: 2009-08-07 00:00:00\n"
                "add: timerange:AN; level:G00; product:GRIB1,200,140,229\n"
                "add: timerange:GRIB1,1; level:MSL; product:GRIB1,80,2,2\n"
        );

        metadata::Collection mdc(qm, Matcher());
        ensure_equals(mdc.size(), 2u);
        ensure(mdc[0].has_source());
        ensure(mdc[1].has_source());

        Summary s;
        qm.query_summary(Matcher::parse(""), s);
        ensure_equals(s.count(), 2u);
    }
    {
        Querymacro qm(cfg, dispatch_cfg, "gridspace", 
                "dataset: testds\n"
                "addtimes: 2009-08-07 00:00:00 2009-08-08 00:00:00 86400\n"
                "add: timerange:AN; level:G00; product:GRIB1,200,140,229\n"
                "add: timerange:GRIB1,1; level:MSL; product:GRIB1,80,2,2\n"
        );

        metadata::Collection mdc(qm, Matcher());
        ensure_equals(mdc.size(), 2u);
        ensure(mdc[0].has_source());
        ensure(mdc[1].has_source());

        Summary s;
        qm.query_summary(Matcher::parse(""), s);
        ensure_equals(s.count(), 2u);
    }
}

// Try "expa" matchers with inline option
def_test(7)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "expa",
            "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
            "ds:testds. d:2009-08-07. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n"
            );

    metadata::Collection mdc(qm, dataset::DataQuery("", true));
    wassert(actual(mdc.size()) == 2u);
    // Ensure that data is reachable
    wassert(actual(mdc[0].getData().size()) == mdc[0].data_size());
    wassert(actual(mdc[1].getData().size()) == mdc[1].data_size());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 2u);
}

// Try "expa" matchers with sort option
// TODO: ensure sorting
def_test(8)
{
    ConfigFile cfg;
    Querymacro qm(cfg, dispatch_cfg, "expa",
            "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
            "ds:testds. d:2009-08-08. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n"
            );

    dataset::DataQuery dq;
    dq.sorter = sort::Compare::parse("month:-reftime");
    metadata::Collection mdc(qm, dq);
    ensure_equals(mdc.size(), 2u);
    ensure(mdc[0].has_source());
    wassert(actual_type(mdc[0].get(TYPE_REFTIME)) == Reftime::decodeString("2009-08-08 00:00:00"));
    ensure(mdc[1].has_source());
    wassert(actual_type(mdc[1].get(TYPE_REFTIME)) == Reftime::decodeString("2009-08-07 00:00:00"));

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 2u);
}

}
