#include "arki/types/tests.h"
#include "arki/types/reftime.h"
#include "intern.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::types;
using namespace arki::summary;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_summary_intern");

void Tests::register_tests() {

// Test basic operations
add_method("basic", [] {
    TypeIntern intern;

    const Type* val1 = intern.intern(Reftime::createPosition(Time(2015, 1, 5, 12, 0, 0)));
    wassert(actual_type(val1).is_reftime_position({2015, 1, 5, 12, 0, 0}));

    const Type* val2 = intern.intern(Reftime::createPosition(Time(2015, 1, 5, 12, 0, 0)));
    wassert(actual(val1 == val2).istrue());

    val2 = intern.lookup(*Reftime::createPosition(Time(2015, 1, 5, 12, 0, 0)));
    wassert(actual(val1 == val2).istrue());

    val2 = intern.lookup(*Reftime::createPosition(Time(2015, 1, 6, 12, 0, 0)));
    wassert(actual(val2).isfalse());

    const Type* val3 = intern.intern(Reftime::createPosition(Time(2015, 1, 6, 12, 0, 0)));
    wassert(actual_type(val3).is_reftime_position({2015, 1, 6, 12, 0, 0}));
    wassert(actual(val1 != val2).istrue());
});

}

}
