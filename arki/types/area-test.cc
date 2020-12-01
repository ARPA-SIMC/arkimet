#include "tests.h"
#include "area.h"
#include "arki/core/binary.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Area>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_area");

void Tests::register_tests() {

// Check GRIB
add_generic_test(
        "grib",
        {},
        "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
        { "GRIB(dieci=10,undici=11,dodici=-12)", },
        "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

// Check two more samples
add_generic_test(
        "grib_1",
        {},
        "GRIB(count=1,pippo=pippo)",
        { "GRIB(count=2,pippo=pippo)", });

// Check ODIMH5
add_generic_test(
        "odimh5",
        {},
        "ODIMH5(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
        { "ODIMH5(dieci=10,undici=11,dodici=-12)", },
        "ODIMH5:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

add_generic_test(
        "vm2",
        {},
        "VM2(1)",
        { "VM2(2)", },
        "VM2,1:lat=4460016, lon=1207738, rep=locali");

add_method("vm2_derived_lookup", [] {
    skip_unless_vm2();
    using namespace arki::types;
    auto vb1 = types::ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
    auto vb2 = types::ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");

    // Create without derived values
    std::unique_ptr<Area> p1 = Area::createVM2(1);
    auto v1 = dynamic_cast<const area::VM2*>(p1.get());

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
        p2 = decodeInner(arki::TYPE_AREA, dec);
    }
    auto v2 = dynamic_cast<const area::VM2*>(p2.get());

    // Try lookup, it should preserve the values that were transmitted
    wassert_true(v2->derived_values() != vb1);
    wassert_true(v2->derived_values() == vb2);

    // It should also be able to retransmit them in turn
    {
        std::vector<uint8_t> full;
        core::BinaryEncoder enc(full);
        p2->encodeWithoutEnvelope(enc);

        core::BinaryDecoder dec(full);
        p2 = decodeInner(arki::TYPE_AREA, dec);
    }
    v2 = dynamic_cast<const area::VM2*>(p2.get());
    wassert_true(v2->derived_values() != vb1);
    wassert_true(v2->derived_values() == vb2);

    // And it looks up again after being transmitted without them
    {
        std::vector<uint8_t> full;
        core::BinaryEncoder enc(full);
        p1->encode_for_indexing(enc);

        core::BinaryDecoder dec(full);
        p2 = decodeInner(arki::TYPE_AREA, dec);
    }
    v2 = dynamic_cast<const area::VM2*>(p2.get());
    wassert_true(v2->derived_values() == vb1);
    wassert_true(v2->derived_values() != vb2);
});


add_method("vm2_derived_encoding", [] {
    skip_unless_vm2();
    using namespace arki::types;
    std::unique_ptr<Area> o = Area::createVM2(1);

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
        pfull = decodeInner(arki::TYPE_AREA, dec);
    }
    {
        core::BinaryDecoder dec(part);
        ppart = decodeInner(arki::TYPE_AREA, dec);
    }
    wassert(actual(pfull) == ppart);
});


}

}
