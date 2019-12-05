#include "arki/tests/tests.h"
#include "arki/matcher/utils.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::matcher;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_utils");

void Tests::register_tests() {

add_method("optionalcommalist", [] {
    OptionalCommaList l("CIAO,,1,2,,3");
    wassert_true(l.has(0));
    wassert_false(l.has(1));
    wassert_true(l.has(2));
    wassert_true(l.has(3));
    wassert_false(l.has(4));
    wassert_true(l.has(5));
    wassert_false(l.has(6));
    wassert_false(l.has(100));

    wassert(actual(l.getInt(1, 100)) == 100);
    wassert(actual(l.getInt(2, 100)) == 1);
    wassert(actual(l.getInt(3, 100)) == 2);
    wassert(actual(l.getInt(4, 100)) == 100);
    wassert(actual(l.getInt(5, 100)) == 3);
    wassert(actual(l.getInt(6, 100)) == 100);
    wassert(actual(l.getInt(100, 100)) == 100);

    wassert(actual(l.getDouble(1,100)) ==   100.0);
    wassert(actual(l.getDouble(2,100)) ==   1.0);
    wassert(actual(l.getDouble(3,100)) ==   2.0);
    wassert(actual(l.getDouble(4,100)) ==   100.0);
    wassert(actual(l.getDouble(5,100)) ==   3.0);
    wassert(actual(l.getDouble(6,100)) ==   100.0);
    wassert(actual(l.getDouble(100,100)) == 100.0);
});

add_method("commajoiner", [] {
    CommaJoiner j;
    j.add("ciao");
    j.addUndef();
    j.add(3);
    j.add(3.14);
    j.addUndef();
    j.addUndef();

    wassert(actual(j.join()) == "ciao,,3,3.14");
});

}

}
