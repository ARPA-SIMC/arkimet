#include "arki/dataset/tests.h"
#include "arki/types/reftime.h"
#include "arki/runtime/config.h"
#include "arki/sort.h"
#include "querymacro.h"
#include "summary.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    ConfigFile dispatch_cfg;

    void test_setup()
    {
        runtime::readMatcherAliasDatabase();

        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            unique = origin, reftime
        )");

        dispatch_cfg.clear();
        dispatch_cfg.mergeInto("testds", cfg);
    }

    void import()
    {
        auto writer(config().create_writer());
        metadata::TestCollection mdc("inbound/test.grib1");
        for (const auto& const_md: mdc)
        {
            Metadata md = *const_md;
            for (unsigned i = 7; i <= 9; ++i)
            {
                md.set(Reftime::createPosition(Time(2009, 8, i, 0, 0, 0)));
                wassert(actual(*writer).import(md));
            }
        }
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_querymacro");

void Tests::register_tests() {

// Test running queries from Lua
add_method("count", [](Fixture& f) {
    f.import();

    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "test0", "foo");

    lua_getglobal(*qm.L, "count1");
    int count1 = lua_tointeger(*qm.L, -1);

    lua_getglobal(*qm.L, "count2");
    int count2 = lua_tointeger(*qm.L, -1);

    lua_pop(*qm.L, 2);

    wassert(actual(count1) == 9);
    wassert(actual(count2) == 3);
});

// Lua script that simply passes through the queries
add_method("noop", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "noop", "testds");

    metadata::Collection mdc(qm, Matcher());
    wassert(actual(mdc.size()) == 9u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());
    ensure(mdc[2].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    wassert(actual(s.count()) == 9u);
});

// Lua script that simply passes through the queries, making temporary copies of data
add_method("noopcopy", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "noopcopy", "testds");

    metadata::Collection mdc(qm, Matcher());
    wassert(actual(mdc.size()) == 9u);
    ensure(mdc[0].has_source());
    ensure(mdc[1].has_source());
    ensure(mdc[2].has_source());

    Summary s;
    qm.query_summary(Matcher::parse(""), s);
    wassert(actual(s.count()) == 9u);
});

// Try "expa" matchers
add_method("expa", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "expa", 
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
});

// Try "expa" matchers with parameter
add_method("expa_arg", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "expa 2009-08-08", 
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
});


// Try "gridspace" matchers
add_method("gridspace", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    {
        Querymacro qm(cfg, f.dispatch_cfg, "gridspace", 
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
        Querymacro qm(cfg, f.dispatch_cfg, "gridspace", 
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
});

// Try "expa" matchers with inline option
add_method("expa_inline", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "expa",
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
});

// Try "expa" matchers with sort option
// TODO: ensure sorting
add_method("expa_sort", [](Fixture& f) {
    f.import();
    ConfigFile cfg;
    Querymacro qm(cfg, f.dispatch_cfg, "expa",
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
});

}

}
