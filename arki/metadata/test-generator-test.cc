#include "arki/tests/tests.h"
#include "arki/types/origin.h"
#include "arki/metadata.h"
#include "test-generator.h"
#include "collection.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::metadata;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_test_generator");

void Tests::register_tests() {

add_method("grib1", [] {
    test::Generator g("grib1");
    Collection c;
    g.generate(c.inserter_func());
    wassert(actual(c.size()) == 1u);
    wassert(actual(c[0].get<types::Origin>()->style()) == types::Origin::Style::GRIB1);
});

add_method("grib2", [] {
    test::Generator g("grib2");
    Collection c;
    g.generate(c.inserter_func());
    wassert(actual(c.size()) == 1u);
    wassert(actual(c[0].get<types::Origin>()->style()) == types::Origin::Style::GRIB2);
});

add_method("bufr", [] {
    test::Generator g("bufr");
    Collection c;
    g.generate(c.inserter_func());
    wassert(actual(c.size()) == 1u);
    wassert(actual(c[0].get<types::Origin>()->style()) == types::Origin::Style::BUFR);
});

add_method("odimh5", [] {
    test::Generator g("odimh5");
    Collection c;
    g.generate(c.inserter_func());
    wassert(actual(c.size()) == 1u);
    wassert(actual(c[0].get<types::Origin>()->style()) == types::Origin::Style::ODIMH5);
});

add_method("grib1_extratypes", [] {
    test::Generator g("grib1");
    g.add(TYPE_ORIGIN, "GRIB1(98, 0, 10)");
    g.add(TYPE_ORIGIN, "GRIB1(200, 0, 10)");
    g.add(TYPE_ORIGIN, "GRIB1(80, 0, 10)");
    g.add(TYPE_PRODUCT, "GRIB1(98, 1, 22)");
    g.add(TYPE_PRODUCT, "GRIB1(98, 1, 33)");
    g.add(TYPE_LEVEL, "GRIB1(100, 0)");

    Collection c;
    g.generate(c.inserter_func());
    wassert(actual(c.size()) == 6u);
});

}

}
