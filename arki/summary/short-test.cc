#include "arki/types/tests.h"
#include "arki/metadata.h"
#include "arki/types.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/run.h"
#include "arki/emitter/json.h"
#include "short.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using arki::core::Time;

struct Fixture : public arki::utils::tests::Fixture
{
    Summary summary;
    Metadata md1;
    Metadata md2;

    Fixture()
    {
    }

    void test_setup()
    {
        md1.clear();
        md1.set(Origin::createGRIB1(1, 2, 3));
        md1.set(Product::createGRIB1(1, 2, 3));
        md1.set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md1.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
        md1.set_source(Source::createInline("grib1", 10));

        md2.clear();
        md2.set(Origin::createGRIB1(3, 4, 5));
        md2.set(Product::createGRIB1(2, 3, 4));
        md2.set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md2.set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
        md2.set_source(Source::createInline("grib1", 20));

        summary.clear();
        summary.add(md1);
        summary.add(md2);
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_summary_short");

void Tests::register_tests() {

add_method("json", [](Fixture& f) {
    summary::Short shrt;
    f.summary.visit(shrt);

    stringstream ss;
    emitter::JSON json(ss);
    shrt.serialise(json);

    wassert(actual(ss.str()) == R"({"items":{"summarystats":{"b":[2006,5,4,3,2,1],"e":[2007,1,2,3,4,5],"c":2,"s":30},"origin":[{"t":"origin","s":"GRIB1","ce":1,"sc":2,"pr":3},{"t":"origin","s":"GRIB1","ce":3,"sc":4,"pr":5}],"product":[{"t":"product","s":"GRIB1","or":1,"ta":2,"pr":3},{"t":"product","s":"GRIB1","or":2,"ta":3,"pr":4}],"timerange":[{"t":"timerange","s":"GRIB1","ty":1,"un":0,"p1":0,"p2":0}]}})");
});

add_method("yaml", [](Fixture& f) {
    summary::Short shrt;
    f.summary.visit(shrt);

    stringstream ss;
    shrt.write_yaml(ss);

    wassert(actual(ss.str()) == R"(SummaryStats:
  Size: 30
  Count: 2
  Reftime: 2006-05-04T03:02:01Z to 2007-01-02T03:04:05Z
Items:
  Origin:
    GRIB1(001, 002, 003)
    GRIB1(003, 004, 005)
  Product:
    GRIB1(001, 002, 003)
    GRIB1(002, 003, 004)
  Timerange:
    GRIB1(001)
)");
});

}

}
