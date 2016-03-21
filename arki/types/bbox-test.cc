#include <arki/types/tests.h>
#include <arki/types/bbox.h>
#include <arki/tests/lua.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

def_tests(arki_types_bbox);

void Tests::register_tests() {

// Check INVALID
add_method("invalid", [] {
    tests::TestGenericType t("bbox", "INVALID");
    wassert(t.check());
});

#ifdef HAVE_LUA
// Test Lua functions
add_method("lua", [] {
    unique_ptr<BBox> o = BBox::createInvalid();

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'INVALID' then return 'style is '..o.style..' instead of INVALID' end \n"
		"  if tostring(o) ~= 'INVALID()' then return 'tostring gave '..tostring(o)..' instead of INVALID()' end \n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
});
#endif

}

}
