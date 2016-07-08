#include "arki/tests/tests.h"
#include "arki/dataset/step.h"
#include "arki/types/reftime.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/utils/pcounter.h"
#include <memory>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::tests;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    Metadata md;

    Fixture()
    {
        md.set("reftime", "2007-06-05T04:03:02Z");
    }
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
    unique_ptr<Step> step(Step::create("yearly"));

    ensure_equals((*step)(f.md), "20/2007");
    ensure(step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2006"))));
    ensure(step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<=2008"))));
    ensure(not step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2007"))));
    ensure(not step->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<2007"))));
});

add_method("monthly", [](Fixture& f) {
    unique_ptr<Step> step(Step::create("monthly"));

    wassert(actual((*step)(f.md)) == "2007/06");
});

add_method("biweekly", [](Fixture& f) {
    unique_ptr<Step> step(Step::create("biweekly"));

    wassert(actual((*step)(f.md)) == "2007/06-1");
});

add_method("weekly", [](Fixture& f) {
    unique_ptr<Step> step(Step::create("weekly"));

    wassert(actual((*step)(f.md)) == "2007/06-1");
});

add_method("daily", [](Fixture& f) {
    unique_ptr<Step> step(Step::create("daily"));

    wassert(actual((*step)(f.md)) == "2007/06-05");
});

}

}
