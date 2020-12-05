#include "arki/types/tests.h"
#include "values.h"
#include "arki/core/binary.h"
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::types::values;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_types_values");

void Tests::register_tests() {

// Check comparison
#if 0
add_method("comparison", [] {
    std::unique_ptr<Value> vi1(Value::create_integer("name", -1));
    std::unique_ptr<Value> vi2(Value::create_integer("name", -1));
    std::unique_ptr<Value> vi3(Value::create_integer("name", 1));
    std::unique_ptr<Value> vs1(Value::create_string("name", "antani"));
    std::unique_ptr<Value> vs2(Value::create_string("name", "antani"));
    std::unique_ptr<Value> vs3(Value::create_string("name", "blinda"));

    wassert_true(*vi1 == *vi1);
    wassert_true(*vi1 == *vi2);
    wassert_true(*vi1 != *vi3);
    wassert_true(*vi2 != *vi3);

    wassert_true(*vs1 == *vs1);
    wassert_true(*vs1 == *vs2);
    wassert_true(*vs1 != *vs3);
    wassert_true(*vs2 != *vs3);

    wassert(actual(vi1->compare(*vi3)) < 0);
    wassert(actual(vi1->compare(*vi1)) == 0);
    wassert(actual(vi3->compare(*vi1)) > 0);
    wassert(actual(vs1->compare(*vs3)) < 0);
    wassert(actual(vs1->compare(*vs1)) == 0);
    wassert(actual(vs3->compare(*vs1)) > 0);
});
#endif

// Check encoding
add_method("encode_int", [] {
    auto encode = [](int value) {
        std::vector<uint8_t> buf;
        core::BinaryEncoder enc(buf);
        values::encode_int(enc, value);
        return buf;
    };

    auto test_codec = [&](int value, unsigned encsize) {
        auto enc = encode(value);
        wassert(actual(enc.size()) == encsize);

        core::BinaryDecoder dec(enc);
        uint8_t lead = dec.pop_byte("lead byte");
        int decoded = values::decode_int(dec, lead);
        wassert(actual(decoded) == value);

        wassert(actual(dec.size) == 0u);
    };

    wassert(test_codec(0, 1u));
    wassert(test_codec(1, 1u));
    wassert(test_codec(-1, 1u));
    wassert(test_codec(30, 1u)); // unsigned that fits in 6 bits
    wassert(test_codec(-31, 1u)); // signed that fits in 6 bits
    wassert(test_codec(1000000, 4u));
    wassert(test_codec(-1234567, 4u));
    // Not 6 bits, but 1 byte
    wassert(test_codec(43, 2u));
    wassert(test_codec(-43, 2u));
    // 2 bytes
    wassert(test_codec(10000, 3u));
    wassert(test_codec(-10000, 3u));
});


#if 0
#warning reenable this
add_method("encoding", [] {
    std::unique_ptr<Value> empty(Value::create_string("name", ""));
    std::unique_ptr<Value> onechar(Value::create_string("name", "a"));
    std::unique_ptr<Value> numstr(Value::create_string("name", "12"));
    std::unique_ptr<Value> longname(Value::create_string("name", "thisIsAVeryLongNameButFitsIn64byesBecauseIts55BytesLong"));
    std::unique_ptr<Value> escaped(Value::create_string("name", "\"\\\'pippo"));

    auto test = [](const Value& var, unsigned encsize) {
        encsize += 1 + 4; // 4 for the name, 1 for its length
        std::unique_ptr<Value> v;
        vector<uint8_t> enc;
        core::BinaryEncoder e(enc);
        wassert(var.encode(e));
        wassert(actual(enc.size()) == encsize);
        core::BinaryDecoder dec(enc);
        wassert(v = Value::decode(dec));
        size_t decsize = dec.buf - enc.data();
        wassert(actual(decsize) == (encsize));
        wassert_true(*v == var);

        std::string enc1 = wcallchecked(var.toString());
        wassert(v = Value::parse("name", enc1));
        wassert_true(*v == var);
    };

    wassert(test(*empty, 1u));
    wassert(test(*onechar, 2u));
    wassert(test(*numstr, 3u));
    wassert(test(*longname, 56u));
    wassert(test(*escaped, 9u));
});
#endif

// Check ValueBag
add_method("valuebag", [] {
    ValueBag v1;
    ValueBag v2;

    v1.set("test1", 1);
    v1.set("test2", 1000000);
    v1.set("test3", -20);
    v1.set("test4", "1");

#if 0
    // Test accessors
    std::unique_ptr<Value> val;
    val = Value::create_integer("test1", 1);
    wassert_true(*v1.test_get("test1") == *val);
    val = Value::create_integer("test2", 1000000);
    wassert_true(*v1.test_get("test2") == *val);
    val = Value::create_integer("test3", -20);
    wassert_true(*v1.test_get("test3") == *val);
    val = Value::create_string("test4", "1");
    wassert_true(*v1.test_get("test4") == *val);
#endif

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 0u);

    // Test copy and comparison
    wassert_true(v1 != v2);

    v2.clear();
    v2.set("test1", 1);
    v2.set("test2", 1000000);
    v2.set("test3", -20);
    v2.set("test4", "1");

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 4u);

    wassert(actual(v1) == v2);

    // Test clear
    v2.clear();
    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 0u);

    // Test encoding and decoding
    std::vector<uint8_t> enc;
    core::BinaryEncoder e(enc);

    v1.encode(e);
    core::BinaryDecoder dec(enc);
    v2 = ValueBag::decode(dec);
    wassert(actual(v1) == v2);

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 4u);

    v2.clear();
    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 0u);

    string enc1 = v1.toString();
    wassert(actual(enc1) == "test1=1, test2=1000000, test3=-20, test4=\"1\"");
    v2 = ValueBag::parse(enc1);

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 4u);

    wassert(actual(v2) == v1);
});

// Check a case where ValueBag seemed to fail (but it actually didn't, the
// problem was elsewhere)
add_method("regression1", [] {
    ValueBag v1;
    auto v2 = ValueBagMatcher::parse("sta=88");

    v1.set("blo", 10);
    v1.set("lat", 5480000);
    v1.set("lon", 895000);
    v1.set("sta", 22);

    wassert_false(v2.is_subset(v1));
});

}

}
