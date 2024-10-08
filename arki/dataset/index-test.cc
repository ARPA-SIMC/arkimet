#include "arki/dataset/tests.h"
#include "arki/dataset/session.h"
#include "arki/dataset/simple.h"
#include "arki/matcher/parser.h"
#include "index.h"
#include "index/manifest.h"
#include "arki/summary.h"
#include "arki/utils/sys.h"
#include "arki/types/source/blob.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::dataset::index;

namespace {

template<typename FIXTURE>
class Tests : public FixtureTestCase<FIXTURE>
{
    using FixtureTestCase<FIXTURE>::FixtureTestCase;

    void register_tests() override;
};

struct BaseFixture : public Fixture
{
    std::shared_ptr<dataset::Session> session;
    metadata::TestCollection mdc;

    void test_setup()
    {
        session = std::make_shared<dataset::Session>();
        mdc.clear();
        mdc.scan_from_file("inbound/fixture.grib1", false);
        sys::rmtree_ifexists("testds");
        std::filesystem::create_directory("testds");
    }
    //void test_teardown() { sys::rmtree_ifexists("testds"); }

};

struct ManifestFixture : public BaseFixture
{
    virtual ~ManifestFixture() {}

    virtual std::unique_ptr<Manifest> create() = 0;

    std::unique_ptr<dataset::Index> make_index()
    {
        auto res = create();
        res->openRW();
        Summary sum; sum.add(mdc[0]);
        res->acquire("2007/07-08.grib", 100, sum);
        sum.clear(); sum.add(mdc[1]);
        res->acquire("2007/07-07.grib", 100, sum);
        sum.clear(); sum.add(mdc[2]);
        res->acquire("2007/10-09.grib", 100, sum);
        return std::unique_ptr<dataset::Index>(res.release());
    }
};

struct ManifestPlainFixture : ManifestFixture
{
    std::unique_ptr<Manifest> create() override
    {
        core::cfg::Section cfg;
        cfg.set("name", "testds");
        cfg.set("path", "testds");
        cfg.set("index_type", "plain");
        cfg.set("locking", "yes");
        cfg.set("step", "daily");

        auto session = std::make_shared<dataset::Session>();
        auto ds = std::make_shared<dataset::simple::Dataset>(session, cfg);
        return Manifest::create(ds, "plain");
    }
};

struct ManifestSqliteFixture : ManifestFixture
{
    std::unique_ptr<Manifest> create() override
    {
        core::cfg::Section cfg;
        cfg.set("name", "testds");
        cfg.set("path", "testds");
        cfg.set("index_type", "sqlite");
        cfg.set("locking", "yes");
        cfg.set("step", "daily");

        auto session = std::make_shared<dataset::Session>();
        auto ds = std::make_shared<dataset::simple::Dataset>(session, cfg);
        return Manifest::create(ds, "sqlite");
    }
};

Tests<ManifestPlainFixture>  test1("arki_dataset_index_tests_manifest_plain");
Tests<ManifestSqliteFixture> test2("arki_dataset_index_tests_manifest_sqlite");


template<class FIXTURE>
void Tests<FIXTURE>::register_tests() {

typedef FIXTURE Fixture;

this->add_method("has_segment", [](Fixture& f) {
    auto idx = f.make_index();

    wassert(actual(idx->has_segment("2007/07-07.grib")).istrue());
    wassert(actual(idx->has_segment("2007/07-08.grib")).istrue());
    wassert(actual(idx->has_segment("2007/10-09.grib")).istrue());
    wassert(actual(idx->has_segment("2007/07-09.grib")).isfalse());
});

this->add_method("segment_timespan", [](Fixture& f) {
    auto idx = f.make_index();

    core::Interval interval;
    wassert(actual(idx->segment_timespan("2007/07-07.grib", interval)).istrue());
    wassert(actual(interval.begin.to_sql()) == "2007-07-07 00:00:00");
    wassert(actual(interval.end.to_sql()) == "2007-07-07 00:00:01");
});

this->add_method("list_segments", [](Fixture& f) {
    auto idx = f.make_index();

    vector<string> res;
    idx->list_segments([&](const std::string& str) { res.push_back(str); });

    wassert(actual(res.size()) == 3u);
    wassert(actual(res[0]) == "2007/07-07.grib");
    wassert(actual(res[1]) == "2007/07-08.grib");
    wassert(actual(res[2]) == "2007/10-09.grib");
});

this->add_method("list_segments_filtered", [](Fixture& f) {
    auto idx = f.make_index();

    auto query = [&](const std::string& matcher) {
        matcher::Parser parser;
        Matcher m = parser.parse(matcher);
        vector<string> res;
        idx->list_segments_filtered(m, [&](const std::string& str) { res.push_back(str); });
        return res;
    };

    auto res = query("reftime:<2007-10-09");
    wassert(actual(res.size()) == 2u);
    wassert(actual(res[0]) == "2007/07-07.grib");
    wassert(actual(res[1]) == "2007/07-08.grib");

    res = query("reftime:<=2007-10-09");
    wassert(actual(res.size()) == 3u);
    wassert(actual(res[0]) == "2007/07-07.grib");
    wassert(actual(res[1]) == "2007/07-08.grib");
    wassert(actual(res[2]) == "2007/10-09.grib");

    res = query("reftime:>2007-07-07");
    wassert(actual(res.size()) == 2u);
    wassert(actual(res[0]) == "2007/07-08.grib");
    wassert(actual(res[1]) == "2007/10-09.grib");

    res = query("reftime:>=2007-07-07");
    wassert(actual(res.size()) == 3u);
    wassert(actual(res[0]) == "2007/07-07.grib");
    wassert(actual(res[1]) == "2007/07-08.grib");
    wassert(actual(res[2]) == "2007/10-09.grib");

    res = query("reftime:=2007-07-08");
    wassert(actual(res.size()) == 1u);
    wassert(actual(res[0]) == "2007/07-08.grib");
});

}

}
