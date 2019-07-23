#include "arki/dataset/tests.h"
#include "arki/metadata.h"
#include "arki/core/file.h"
#include "arki/matcher.h"
#include "gridquery.h"
#include "memory.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::types;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            unique = origin, reftime
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_gridquery");

void Tests::register_tests() {

add_method("gridquery", [](Fixture& f) {
    f.import();
    auto reader = f.config().create_reader();
    dataset::GridQuery gq(*reader);

    // Trivially query only one item
    wassert(gq.add(Matcher::parse("origin:GRIB1,200,0,101;product:GRIB1,200,140,229")));
    wassert(gq.addTime(Time(2007, 7, 8, 13, 0, 0)));
    wassert(gq.consolidate());

    wassert(actual(gq.expectedItems()) == 1u);

    ItemSet is;
    is.set("origin", "GRIB1(200, 0, 101)");
    is.set("product", "GRIB1(200, 140, 229)");
    is.set("reftime", "2007-07-08 13:00:00");

    wassert(actual(gq.satisfied()).isfalse());
    wassert(actual(gq.checkAndMark(is)).istrue());
    wassert(actual(gq.checkAndMark(is)).isfalse());
    wassert(actual(gq.satisfied()).istrue());
});

// Test adding an entry which does not expand to anything
add_method("no_expansion", [](Fixture& f) {
    // Build a test dataset
    string md_yaml(
        "Source: BLOB(grib1,/dev/null:0+186196)\n"
        "Origin: GRIB1(200, 255, 047)\n"
        "Product: GRIB1(200, 002, 061)\n"
        "Level: GRIB1(001)\n"
        "Timerange: GRIB1(004, 000h, 012h)\n"
        "Reftime: 2010-05-03T00:00:00Z\n"
        "Area: GRIB(Ni=297, Nj=313, latfirst=-25000000, latlast=-5500000, latp=-32500000, lonfirst=-8500000, lonlast=10000000, lonp=10000000, rot=0, type=10)\n"
        "Run: MINUTE(00:00)\n"
    );
    auto reader = LineReader::from_chars(md_yaml.data(), md_yaml.size());
    unique_ptr<Metadata> md = Metadata::create_empty();
    md->readYaml(*reader, "(test memory buffer)");

    dataset::Memory ds;
    ds.acquire(move(md));

    // Build the grid query
    dataset::GridQuery gq(ds);

    try {
        gq.add(Matcher::parse("timerange:c012; level:g00; product:u"));
        ensure(false);
    } catch (std::exception& c) {
        ensure(string(c.what()).find("no data which correspond to the matcher") != string::npos);
    }
    gq.add(Matcher::parse("timerange:c012; level:g00; product:tp"));

    Time t(2010, 05, 03, 0, 0, 0);
    gq.addTime(t);

    gq.consolidate();

    ensure_equals(gq.expectedItems(), 1u);
});

}

}
