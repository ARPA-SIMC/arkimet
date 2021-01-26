#include "tests.h"
#include "typeset.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_types_typeset");

void Tests::register_tests() {

add_method("typeset", []() {
    TypeSet v;
    // Operations on empty
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0u);
    wassert(actual(v == TypeSet()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());

    // Add a null and a valid entry
    auto type = decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z");
    const Type* sample = type.get();
    wassert(actual(v.insert(move(type)) == sample).istrue());
    wassert(actual(type.get()).isfalse());
    wassert(actual(v.empty()).isfalse());
    wassert(actual(v.size()) == 1u);
    wassert(actual(v != TypeSet()).istrue());

    // Lookup
    wassert(actual(v.find(*decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z")) == sample).istrue());
    wassert(actual(v.find(*decodeString(TYPE_REFTIME, "2015-01-05T11:00:00Z")) == sample).isfalse());

    // Iteration
    {
        TypeSet::const_iterator i = v.begin();
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).is_reftime_position({2015, 1, 5, 12, 0, 0}));
        ++i;
        wassert(actual(i == v.end()).istrue());
    }

    // Insert again, we get the previous pointer
    wassert(actual(v.insert(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z")) == sample).istrue());

    // Unset, we are again empty
    v.erase(*sample);
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0u);
    wassert(actual(v == TypeSet()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());
});

}

}

