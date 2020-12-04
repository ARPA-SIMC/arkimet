#include "tests.h"
#include "product.h"
#include "arki/core/binary.h"

namespace {
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
    std::unique_ptr<Product> o = Product::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Product::Style::GRIB1);
    unsigned ori, tab, pro;
    o->get_GRIB1(ori, tab, pro);
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
    std::unique_ptr<Product> o;
    unsigned ce, di, ca, nu, ta, lo;

    o = Product::createGRIB2(1, 2, 3, 4);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    o->get_GRIB2(ce, di, ca, nu, ta, lo);
    wassert(actual(ce) == 1u);
    wassert(actual(di) == 2u);
    wassert(actual(ca) == 3u);
    wassert(actual(nu) == 4u);
    wassert(actual(ta) == 4u);
    wassert(actual(lo) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    o->get_GRIB2(ce, di, ca, nu, ta, lo);
    wassert(actual(ce) == 1u);
    wassert(actual(di) == 2u);
    wassert(actual(ca) == 3u);
    wassert(actual(nu) == 4u);
    wassert(actual(ta) == 5u);
    wassert(actual(lo) == 255u);

    o = Product::createGRIB2(1, 2, 3, 4, 4, 5);
    wassert(actual(o->style()) == Product::Style::GRIB2);
    o->get_GRIB2(ce, di, ca, nu, ta, lo);
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
    vb.set("name", "antani");
    std::unique_ptr<Product> o = Product::createBUFR(1, 2, 3, vb);
    wassert(actual(o->style()) == Product::Style::BUFR);
    unsigned ty, su, lo;
    ValueBag va;
    o->get_BUFR(ty, su, lo, va);
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
    std::string obj, prod;
    o->get_ODIMH5(obj, prod);
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
    skip_unless_vm2();
    using namespace arki::types;
    std::unique_ptr<Product> o = Product::createVM2(1);
    wassert(actual(o->style()) == Product::Style::VM2);
    unsigned vi;
    o->get_VM2(vi);
    wassert(actual(vi) == 1ul);
});

add_method("vm2_derived_lookup", [] {
    skip_unless_vm2();
    using namespace arki::types;
    ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
    ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");

    // Create without derived values
    std::unique_ptr<Product> p1 = Product::createVM2(1);
    auto v1 = dynamic_cast<const product::VM2*>(p1.get());

    // Try lookup
    wassert_true(v1->derived_values() == vb1);
    wassert_true(v1->derived_values() != vb2);

    // Encode with a different set of derived values, and decode
    std::unique_ptr<Type> p2;
    {
        std::vector<uint8_t> full;
        core::BinaryEncoder enc(full);
        p1->encode_for_indexing(enc);
        vb2.encode(enc);

        core::BinaryDecoder dec(full);
        p2 = Type::decodeInner(arki::TYPE_PRODUCT, dec);
    }
    auto v2 = dynamic_cast<const product::VM2*>(p2.get());

    // Try lookup, it should preserve the values that were transmitted
    wassert_true(v2->derived_values() != vb1);
    wassert_true(v2->derived_values() == vb2);

    // It should also be able to retransmit them in turn
    {
        std::vector<uint8_t> full;
        core::BinaryEncoder enc(full);
        p2->encodeWithoutEnvelope(enc);

        core::BinaryDecoder dec(full);
        p2 = Type::decodeInner(arki::TYPE_PRODUCT, dec);
    }
    v2 = dynamic_cast<const product::VM2*>(p2.get());
    wassert_true(v2->derived_values() != vb1);
    wassert_true(v2->derived_values() == vb2);

    // And it looks up again after being transmitted without them
    {
        std::vector<uint8_t> full;
        core::BinaryEncoder enc(full);
        p1->encode_for_indexing(enc);

        core::BinaryDecoder dec(full);
        p2 = Type::decodeInner(arki::TYPE_PRODUCT, dec);
    }
    v2 = dynamic_cast<const product::VM2*>(p2.get());
    wassert_true(v2->derived_values() == vb1);
    wassert_true(v2->derived_values() != vb2);
});

add_method("vm2_derived_encoding", [] {
    skip_unless_vm2();
    using namespace arki::types;

    std::unique_ptr<Product> o = Product::createVM2(1);

    // Encode with derived values: encodeWithoutEnvelope adds the derived values
    std::vector<uint8_t> full;
    {
        core::BinaryEncoder enc(full);
        o->encodeWithoutEnvelope(enc);
    }

    std::vector<uint8_t> part;
    {
        core::BinaryEncoder enc(part);
        o->encode_for_indexing(enc);
    }
    wassert(actual(full.size()) > part.size());

    // Decode, they are the same
    std::unique_ptr<Type> pfull, ppart;
    {
        core::BinaryDecoder dec(full);
        pfull = Type::decodeInner(arki::TYPE_PRODUCT, dec);
    }
    {
        core::BinaryDecoder dec(part);
        ppart = Type::decodeInner(arki::TYPE_PRODUCT, dec);
    }
    wassert(actual(pfull) == ppart);
});

}

}
