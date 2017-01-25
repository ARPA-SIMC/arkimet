#include "arki/tests/tests.h"
#include "arki/dataset/step.h"
#include "arki/types/reftime.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/utils/pcounter.h"
#include "arki/utils/sys.h"
#include "arki/utils.h"
#include <algorithm>
#include <memory>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    core::Time time;

    Fixture()
        : time(2007, 6, 5, 4, 3, 2)
    {
    }

    // Called before each test
    void test_setup() { sys::mkdir_ifmissing("test_step"); }

    // Called after each test
    void test_teardown() { sys::rmtree("test_step"); }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_step");


inline const matcher::OR& mimpl(const Matcher& m)
{
    return *m.get(TYPE_REFTIME);
}

void Tests::register_tests() {

add_method("yearly", [](Fixture& f) {
    auto step = Step::create("yearly");

    ensure_equals((*step)(f.time), "20/2007");
    ensure(step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2006"))));
    ensure(step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<=2008"))));
    ensure(not step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2007"))));
    ensure(not step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<2007"))));

    sys::mkdir_ifmissing("test_step/19");
    createFlagfile("test_step/19/1998.grib");
    createFlagfile("test_step/19/1998.bufr");
    sys::mkdir_ifmissing("test_step/20");
    createFlagfile("test_step/20/2001.grib");
    createFlagfile("test_step/20/2002.grib");

    vector<string> res;
    step->list_segments("test_step", "grib", Matcher::parse("reftime:<2002"), [&](std::string&& s) { res.emplace_back(move(s)); });
    std::sort(res.begin(), res.end());
    wassert(actual(res.size()) == 2u);
    wassert(actual(res[0]) == "19/1998.grib");
    wassert(actual(res[1]) == "20/2001.grib");
});

add_method("monthly", [](Fixture& f) {
    auto step = Step::create("monthly");

    wassert(actual((*step)(f.time)) == "2007/06");

    sys::mkdir_ifmissing("test_step/2007");
    createFlagfile("test_step/2007/01.grib");
    createFlagfile("test_step/2007/01.bufr");
    sys::mkdir_ifmissing("test_step/2008");
    createFlagfile("test_step/2008/06.grib");
    sys::mkdir_ifmissing("test_step/2008/07.grib");
    sys::mkdir_ifmissing("test_step/2009");
    createFlagfile("test_step/2009/11.grib");
    createFlagfile("test_step/2009/12.grib");

    vector<string> res;
    step->list_segments("test_step", "grib", Matcher::parse("reftime:<2009-11-15"), [&](std::string&& s) { res.emplace_back(move(s)); });
    std::sort(res.begin(), res.end());
    wassert(actual(res.size()) == 4u);
    wassert(actual(res[0]) == "2007/01.grib");
    wassert(actual(res[1]) == "2008/06.grib");
    wassert(actual(res[2]) == "2008/07.grib");
    wassert(actual(res[3]) == "2009/11.grib");
});

add_method("biweekly", [](Fixture& f) {
    auto step = Step::create("biweekly");

    wassert(actual((*step)(f.time)) == "2007/06-1");
});

add_method("weekly", [](Fixture& f) {
    auto step = Step::create("weekly");

    wassert(actual((*step)(f.time)) == "2007/06-1");
});

add_method("daily", [](Fixture& f) {
    auto step = Step::create("daily");

    wassert(actual((*step)(f.time)) == "2007/06-05");

    sys::mkdir_ifmissing("test_step/2007");
    createFlagfile("test_step/2007/01-01.grib");
    createFlagfile("test_step/2007/01-01.bufr");
    sys::mkdir_ifmissing("test_step/2008");
    createFlagfile("test_step/2008/06-01.grib");
    sys::mkdir_ifmissing("test_step/2008/06-05.grib");
    sys::mkdir_ifmissing("test_step/2009");
    createFlagfile("test_step/2009/12-29.grib");
    createFlagfile("test_step/2009/12-30.grib");

    vector<string> res;
    step->list_segments("test_step", "grib", Matcher::parse("reftime:<2009-12-30"), [&](std::string&& s) { res.emplace_back(move(s)); });
    std::sort(res.begin(), res.end());
    wassert(actual(res.size()) == 4u);
    wassert(actual(res[0]) == "2007/01-01.grib");
    wassert(actual(res[1]) == "2008/06-01.grib");
    wassert(actual(res[2]) == "2008/06-05.grib");
    wassert(actual(res[3]) == "2009/12-29.grib");
});

}

}
