#include "config.h"
#include <arki/tests/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/test-generator.h>
#include <arki/metadata/collection.h>
#include <arki/types/origin.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::metadata;
using namespace arki::tests;

struct arki_metadata_test_generator_shar {
    arki_metadata_test_generator_shar()
    {
    }
};
TESTGRP(arki_metadata_test_generator);

// Simple generation
def_test(1)
{
    test::Generator g("grib1");
    Collection c;
    g.generate(c.inserter_func());
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::GRIB1);
}

def_test(2)
{
    test::Generator g("grib2");
    Collection c;
    g.generate(c.inserter_func());
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::GRIB2);
}

def_test(3)
{
    test::Generator g("bufr");
    Collection c;
    g.generate(c.inserter_func());
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::BUFR);
}

def_test(4)
{
    test::Generator g("odimh5");
    Collection c;
    g.generate(c.inserter_func());
    ensure_equals(c.size(), 1u);
    ensure_equals(c[0].get<types::Origin>()->style(), types::Origin::ODIMH5);
}

def_test(5)
{
    test::Generator g("grib1");
    g.add(TYPE_ORIGIN, "GRIB1(98, 0, 10)");
    g.add(TYPE_ORIGIN, "GRIB1(200, 0, 10)");
    g.add(TYPE_ORIGIN, "GRIB1(80, 0, 10)");
    g.add(TYPE_PRODUCT, "GRIB1(98, 1, 22)");
    g.add(TYPE_PRODUCT, "GRIB1(98, 1, 33)");
    g.add(TYPE_LEVEL, "GRIB1(100, 0)");

    Collection c;
    g.generate(c.inserter_func());
    ensure_equals(c.size(), 6u);
}

}
