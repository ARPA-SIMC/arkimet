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
    unsigned ori, tab, pro;
    v->get_GRIB1(ori, tab, pro);
    wassert(actual(ori) == 1u);
    wassert(actual(tab) == 2u);
    wassert(actual(pro) == 3u);
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
    unsigned ce, di, ca, nu, ta, lo;

    o = Product::createGRIB2(1, 2, 3, 4);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    v->get_GRIB2(ce, di, ca, nu, ta, lo);
    wassert(actual(ce) == 1u);
    wassert(actual(di) == 2u);
    wassert(actual(ca) == 3u);
    wassert(actual(nu) == 4u);
    wassert(actual(ta) == 4u);
    wassert(actual(lo) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    v->get_GRIB2(ce, di, ca, nu, ta, lo);
    wassert(actual(ce) == 1u);
    wassert(actual(di) == 2u);
    wassert(actual(ca) == 3u);
    wassert(actual(nu) == 4u);
    wassert(actual(ta) == 5u);
    wassert(actual(lo) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    v = dynamic_cast<product::GRIB2*>(o.get());
    v->get_GRIB2(ce, di, ca, nu, ta, lo);
    wassert(actual(ce) == 1u);
    wassert(actual(di) == 2u);
    wassert(actual(ca) == 3u);
    wassert(actual(nu) == 4u);
    wassert(actual(ta) == 4u);
    wassert(actual(lo) == 5u);
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
    unsigned ty, su, lo;
    ValueBag va;
    v->get_BUFR(ty, su, lo, va);
    wassert(actual(ty) == 1u);
    wassert(actual(su) == 2u);
    wassert(actual(lo) == 3u);
    wassert(actual(va) == vb);
});

// Check ODIMH5
add_generic_test("odimh5",
    { "ODIMH5(obj, pro)", "GRIB2(2, 3, 4, 5)", "ODIMH5(ob, prod)", },
    "ODIMH5(obj, prod)",
    { "ODIMH5(obj1, prod)", "ODIMH5(obj, prod1)", },
    "ODIMH5,obj,prod");

add_method("odim_details", [] {
    using namespace arki::types;
    std::unique_ptr<Product> o = Product::createODIMH5("obj", "prod");
    wassert(actual(o->style()) == Product::Style::ODIMH5);
    product::ODIMH5* v = dynamic_cast<product::ODIMH5*>(o.get());
    std::string obj, prod;
    v->get_ODIMH5(obj, prod);
    wassert(actual(obj) == "obj");
    wassert(actual(prod) == "prod");
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
    unsigned vi;
    v->get_VM2(vi);
    wassert(actual(vi) == 1ul);

    // Test derived values
    ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
    wassert_true(product::VM2::create(1)->derived_values() == vb1);
    ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
    wassert_true(product::VM2::create(1)->derived_values() != vb2);
});

}

}
