#include "tests.h"
#include "product.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Product>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_product");

void Tests::register_tests() {

// Check GRIB1
add_generic_test("grib1",
    { "GRIB1(1, 1, 1)", "GRIB1(1, 2, 2)", },
    "GRIB1(1, 2, 3)",
    { "GRIB1(1, 2, 4)", "GRIB1(2, 3, 4)", "GRIB2(1, 2, 3, 4)", "BUFR(1, 2, 3)", "BUFR(1, 2, 3, name=antani)", },
    "GRIB1,1,2,3");

add_method("grib1_details", [] {
    using namespace arki::types;
    unique_ptr<Product> o = Product::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Product::Style::GRIB1);
    const product::GRIB1* v = dynamic_cast<product::GRIB1*>(o.get());
    wassert(actual(v->origin()) == 1u);
    wassert(actual(v->table()) == 2u);
    wassert(actual(v->product()) == 3u);
});

// Check GRIB2
add_generic_test("grib2",
    { "GRIB1(1, 2, 3)" },
    "GRIB2(1, 2, 3, 4)",
    { "GRIB2(1, 2, 3, 4, 5)", "GRIB2(2, 3, 4, 5)", "BUFR(1, 2, 3)", "BUFR(1, 2, 3, name=antani)", },
    "GRIB2,1,2,3,4");

// Check GRIB2 with different table version
add_generic_test("grib2_table",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4)", },
    "GRIB2(1, 2, 3, 4, 5)",
    { "GRIB2(1, 2, 3, 4, 6)", "GRIB2(2, 3, 4, 5)", "BUFR(1, 2, 3)", "BUFR(1, 2, 3, name=antani)", },
    "GRIB2,1,2,3,4,5");

// Check GRIB2 with different table version or local table version
add_generic_test("grib2_local_table",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 4, 4)" },
    "GRIB2(1, 2, 3, 4, 4, 5)",
    { "GRIB2(1, 2, 3, 4)", "GRIB2(1, 2, 3, 4, 4, 6)", "GRIB2(2, 3, 4, 5)", "BUFR(1, 2, 3)", "BUFR(1, 2, 3, name=antani)", },
    "GRIB2,1,2,3,4,4,5");

add_method("grib2_details", [] {
    using namespace arki::types;
    unique_ptr<Product> o;
    const product::GRIB2* v;

    o = Product::createGRIB2(1, 2, 3, 4);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 4u);
    wassert(actual(v->local_table_version()) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 5u);
    wassert(actual(v->local_table_version()) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 4u);
    wassert(actual(v->local_table_version()) == 5u);
});

// Check BUFR
add_generic_test("bufr",
    { "GRIB1(1, 2, 3)", "GRIB2(2, 3, 4, 5)", "BUFR(1, 2, 3)", },
    "BUFR(1, 2, 3, name=antani)",
    { "BUFR(1, 2, 3, name=antani1)", "BUFR(1, 2, 3, name1=antani)", },
    "BUFR,1,2,3:name=antani");

add_method("bufr_details", [] {
    using namespace arki::types;
    ValueBag vb;
    vb.set("name", values::Value::create_string("antani"));
    unique_ptr<Product> o = Product::createBUFR(1, 2, 3, vb);
    wassert(actual(o->style()) == Product::Style::BUFR);
    product::BUFR* v = dynamic_cast<product::BUFR*>(o.get());
    wassert(actual(v->type()) == 1u);
    wassert(actual(v->subtype()) == 2u);
    wassert(actual(v->localsubtype()) == 3u);
    wassert(actual(v->values()) == vb);

    ValueBag vb2;
    vb2.set("val", values::Value::create_string("blinda"));
    v->addValues(vb2);
    stringstream tmp;
    tmp << *o;
    wassert(actual(tmp.str()) == "BUFR(001, 002, 003, name=antani, val=blinda)");
});

// Check VM2
add_generic_test("vm2",
    { "GRIB1(1, 2, 3)", "GRIB2(2, 3, 4, 5)", "BUFR(1, 2, 3)", "BUFR(1, 2, 3, name=antani)", },
    "VM2(1)",
    { "VM2(2)", },
    "VM2,1:bcode=B20013, l1=0, l2=0, lt1=256, lt2=258, p1=0, p2=0, tr=254, unit=m");

add_method("vm2_details", [] {
    using namespace arki::types;
    unique_ptr<Product> o = Product::createVM2(1);
    wassert(actual(o->style()) == Product::Style::VM2);
    const product::VM2* v = dynamic_cast<product::VM2*>(o.get());
    wassert(actual(v->variable_id()) == 1ul);

    // Test derived values
    ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
    wassert_true(product::VM2::create(1)->derived_values() == vb1);
    ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
    wassert_true(product::VM2::create(1)->derived_values() != vb2);
});

}

}
