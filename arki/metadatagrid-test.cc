#include <arki/tests/tests.h>
#include <arki/metadatagrid.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>

#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadatagrid");

void Tests::register_tests() {

// Test querying the datasets
add_method("query", [] {
    // Create a 2x2 metadata grid
    MetadataGrid mdg;
    mdg.add(*Origin::createGRIB1(200, 0, 101));
    mdg.add(*Origin::createGRIB1(200, 0, 102));
    mdg.add(*Product::createGRIB1(200, 140, 229));
    mdg.add(*Product::createGRIB1(200, 140, 230));
    mdg.consolidate();

    //ensure_equals(mdg.maxidx, 4u);

    Metadata md;
    md.set("origin", "GRIB1(200, 0, 101)");
    md.set("product", "GRIB1(200, 140, 229)");

    ensure_equals(mdg.index(md), 0);
});

}

}
