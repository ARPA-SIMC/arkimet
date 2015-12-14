#include <arki/types/tests.h>
#include <arki/types/value.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_value_shar {
};
TESTGRP(arki_types_value);

// Check text value
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("value", "ciao");
    t.lower.push_back("cia");
    t.higher.push_back("ciap");
    wassert(t);
}

// Check binary value
template<> template<>
void to::test<2>()
{
    tests::TestGenericType t("value", "ciao♥");
    t.lower.push_back("ciao");
    t.higher.push_back("cia♥");
    wassert(t);
}

// Check binary value with zeros
template<> template<>
void to::test<3>()
{
    tests::TestGenericType t("value", string("ci\0ao", 5));
    t.lower.push_back("ci");
    t.higher.push_back("cia♥");
    wassert(t);
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#if 0 // TODO
#ifdef HAVE_LUA
	Item<Run> o = run::Minute::create(12, 30);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'MINUTE' then return 'style is '..o.style..' instead of MINUTE' end \n"
		"  if o.hour ~= 12 then return 'o.hour is '..o.hour..' instead of 12' end \n"
		"  if o.min ~= 30 then return 'o.min is '..o.min..' instead of 30' end \n"
		"  if tostring(o) ~= 'MINUTE(12:30)' then return 'tostring gave '..tostring(o)..' instead of MINUTE(12:30)' end \n"
		"  local o1 = arki_run.minute(12, 30)\n"
		"  if o ~= o1 then return 'new run is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
#endif
}

}
