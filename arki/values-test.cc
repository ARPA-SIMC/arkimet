#include "types/tests.h"
#include "arki/types/tests.h"
#include "arki/values.h"
#include "arki/binary.h"
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_values");

void Tests::register_tests() {

// Check comparison
add_method("comparison", [] {
    std::unique_ptr<Value> vi1(Value::create_integer(-1));
    std::unique_ptr<Value> vi2(Value::create_integer(-1));
    std::unique_ptr<Value> vi3(Value::create_integer(1));
    std::unique_ptr<Value> vs1(Value::create_string("antani"));
    std::unique_ptr<Value> vs2(Value::create_string("antani"));
    std::unique_ptr<Value> vs3(Value::create_string("blinda"));

    wassert_true(*vi1 == *vi1);
    wassert_true(*vi1 == *vi2);
    wassert_true(*vi1 != *vi3);
    wassert_true(*vi2 != *vi3);

    wassert_true(*vs1 == *vs1);
    wassert_true(*vs1 == *vs2);
    wassert_true(*vs1 != *vs3);
    wassert_true(*vs2 != *vs3);

    wassert_true(*vi1 < *vi3);
    wassert_false(*vi1 < *vi1);
    wassert_false(*vi3 < *vi1);
    wassert_true(*vs1 < *vs3);
    wassert_false(*vs1 < *vs1);
    wassert_false(*vs3 < *vs1);
});

// Check encoding
add_method("encoding", [] {
    std::unique_ptr<Value> zero(Value::create_integer(0));
    std::unique_ptr<Value> one(Value::create_integer(1));
    std::unique_ptr<Value> minusOne(Value::create_integer(-1));
    std::unique_ptr<Value> u6bit(Value::create_integer(30));
    std::unique_ptr<Value> s6bit(Value::create_integer(-31));
    std::unique_ptr<Value> onemillion(Value::create_integer(1000000));
    std::unique_ptr<Value> bignegative(Value::create_integer(-1234567));
    std::unique_ptr<Value> empty(Value::create_string(""));
    std::unique_ptr<Value> onechar(Value::create_string("a"));
    std::unique_ptr<Value> numstr(Value::create_string("12"));
    std::unique_ptr<Value> longname(Value::create_string("thisIsAVeryLongNameButFitsIn64byesBecauseIts55BytesLong"));
    std::unique_ptr<Value> escaped(Value::create_string("\"\\\'pippo"));
    // Not 6 bits, but 1 byte
    std::unique_ptr<Value> fourtythree(Value::create_integer(43));
    std::unique_ptr<Value> minusfourtythree(Value::create_integer(-43));
    // 2 bytes
    std::unique_ptr<Value> tenthousand(Value::create_integer(10000));
    std::unique_ptr<Value> minustenthousand(Value::create_integer(-10000));

    auto test = [](const Value& var, unsigned encsize) {
        std::unique_ptr<Value> v;
        vector<uint8_t> enc;
        BinaryEncoder e(enc);
        var.encode(e);
        wassert(actual(enc.size()) == (encsize));
        BinaryDecoder dec(enc);
        v.reset(Value::decode(dec));
        size_t decsize = dec.buf - enc.data();
        wassert(actual(decsize) == (encsize));
        wassert_true(*v == var);

        string enc1 = var.toString();
        v.reset(Value::parse(enc1));
        wassert_true(*v == var);
    };

    test(*zero, 1u);
    test(*one, 1u);
    test(*minusOne, 1u);
    test(*u6bit, 1u);
    test(*s6bit, 1u);
    test(*onemillion, 4u);
    test(*bignegative, 4u);
    test(*empty, 1u);
    test(*onechar, 2u);
    test(*numstr, 3u);
    test(*longname, 56u);
    test(*escaped, 9u);
    test(*fourtythree, 2u);
    test(*minusfourtythree, 2u);
    test(*tenthousand, 3u);
    test(*minustenthousand, 3u);
});

// Check ValueBag
add_method("valuebag", [] {
	ValueBag v1;
	ValueBag v2;
	unique_ptr<Value> val;

    v1.set("test1", Value::create_integer(1));
    v1.set("test2", Value::create_integer(1000000));
    v1.set("test3", Value::create_integer(-20));
    v1.set("test4", Value::create_string("1"));

    // Test accessors
    val.reset(Value::create_integer(1));
    wassert_true(*v1.get("test1") == *val);
    val.reset(Value::create_integer(1000000));
    wassert_true(*v1.get("test2") == *val);
    val.reset(Value::create_integer(-20));
    wassert_true(*v1.get("test3") == *val);
    val.reset(Value::create_string("1"));
    wassert_true(*v1.get("test4") == *val);

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 0u);

    // Test copy and comparison
    wassert_true(v1 != v2);
    v2 = v1;

    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 4u);

    wassert(actual(v1) == v2);

    // Test clear
    v2.clear();
    wassert(actual(v1.size()) == 4u);
    wassert(actual(v2.size()) == 0u);

    // Test encoding and decoding
    std::vector<uint8_t> enc;
    BinaryEncoder e(enc);

    v1.encode(e);
    BinaryDecoder dec(enc);
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
	ValueBag v2;

    v1.set("blo", Value::create_integer(10));
    v1.set("lat", Value::create_integer(5480000));
    v1.set("lon", Value::create_integer(895000));
    v1.set("sta", Value::create_integer(22));

    v2.set("sta", Value::create_integer(88));

    wassert_false(v1.contains(v2));
});

}

}
