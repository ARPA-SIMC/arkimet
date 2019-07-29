#include "arki/tests/legacy.h"
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
	ensure(l.has(0));
	ensure(!l.has(1));
	ensure(l.has(2));
	ensure(l.has(3));
	ensure(!l.has(4));
	ensure(l.has(5));
	ensure(!l.has(6));
	ensure(!l.has(100));

	ensure_equals(l.getInt(1, 100), 100);
	ensure_equals(l.getInt(2, 100), 1);
	ensure_equals(l.getInt(3, 100), 2);
	ensure_equals(l.getInt(4, 100), 100);
	ensure_equals(l.getInt(5, 100), 3);
	ensure_equals(l.getInt(6, 100), 100);
	ensure_equals(l.getInt(100, 100), 100);

    ensure_equals(l.getDouble(1,100),   100.0);
    ensure_equals(l.getDouble(2,100),   1.0);
    ensure_equals(l.getDouble(3,100),   2.0);
    ensure_equals(l.getDouble(4,100),   100.0);
    ensure_equals(l.getDouble(5,100),   3.0);
    ensure_equals(l.getDouble(6,100),   100.0);
    ensure_equals(l.getDouble(100,100),     100.0);
});

add_method("commajoiner", [] {
	CommaJoiner j;
	j.add("ciao");
	j.addUndef();
	j.add(3);
	j.add(3.14);
	j.addUndef();
	j.addUndef();

	ensure_equals(j.join(), "ciao,,3,3.14");
});

}

}
