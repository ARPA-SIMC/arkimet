#include "arki/types/tests.h"
#include "arki/types/reftime.h"
#include "intern.h"

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;
using arki::core::Time;

struct arki_summary_intern_shar {
    arki_summary_intern_shar()
    {
    }
};
TESTGRP(arki_summary_intern);

// Test basic operations
def_test(1)
{
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
}

}
