#include "tests.h"
#include "typevector.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_types_typevector");

void Tests::register_tests() {

add_method("misc", [] {
    TypeVector v;
    // Operations on empty
    wassert(actual(v.empty()).istrue());
    wassert(actual(v.size()) == 0u);
    wassert(actual(v == TypeVector()).istrue());
    wassert(actual(v.begin() == v.end()).istrue());

    // Add a null and a valid entry
    v.set(1, decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));
    wassert(actual(v.empty()).isfalse());
    wassert(actual(v.size()) == 2u);
    wassert(actual(v != TypeVector()).istrue());

    // Lookup
    wassert(actual(v[0]).isfalse());
    wassert(actual(v[1]).istrue());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 5, 12, 0, 0}));

    // Iteration
    {
        TypeVector::const_iterator i = v.begin();
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).isfalse());
        ++i;
        wassert(actual(i != v.end()).istrue());
        wassert(actual_type(*i).is_reftime_position({2015, 1, 5, 12, 0, 0}));
        ++i;
        wassert(actual(i == v.end()).istrue());
    }

    // Replace a non-null entry
    v.set(1, decodeString(TYPE_REFTIME, "2015-01-06T12:00:00Z"));
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Replace a null entry
    v.set(0, decodeString(TYPE_REFTIME, "2015-01-04T12:00:00Z"));
    wassert(actual_type(v[0]).is_reftime_position({2015, 1, 4, 12, 0, 0}));

    // Unset
    v.unset(0);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Resize, enlarging
    v.resize(4);
    wassert(actual(v.size()) == 4u);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
    wassert(actual_type(v[2]).isfalse());
    wassert(actual_type(v[3]).isfalse());

    // Resize, shrinking
    v.set(3, decodeString(TYPE_REFTIME, "2015-01-01T12:00:00Z"));
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
    wassert(actual_type(v[2]).isfalse());
    wassert(actual_type(v[3]).is_reftime_position({2015, 1, 1, 12, 0, 0}));
    v.resize(2);
    wassert(actual(v.size()) == 2u);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Rtrim
    v.set(5, 0);
    wassert(actual(v.size()) == 6u);
    v.rtrim();
    wassert(actual(v.size()) == 2u);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(v[1]).is_reftime_position({2015, 1, 6, 12, 0, 0}));

    // Split
    TypeVector out;
    v.split(1, out);
    wassert(actual(v.size()) == 1u);
    wassert(actual(out.size()) == 1u);
    wassert(actual_type(v[0]).isfalse());
    wassert(actual_type(out[0]).is_reftime_position({2015, 1, 6, 12, 0, 0}));
});

}

}
