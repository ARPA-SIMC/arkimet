#include "arki/tests/tests.h"
#include "arki/utils/lua.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/matcher.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

int alua_test(lua_State* L)
{
    lua_pushinteger(L, 1);
    return 1;
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_lua");

void Tests::register_tests() {

add_method("code", [] {
    // Just run trivial Lua code, and see that nothing wrong happens
    Lua L(true, false);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
    wassert(actual(lua_gettop(L)) == 0);
});

add_method("add_global_library", [] {
    // Test add_global_library
    Lua L(true, false);

    static const struct luaL_Reg lib[] = {
        { "test", alua_test },
        { NULL, NULL }
    };
    utils::lua::add_global_library(L, "testlib", lib);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("if testlib.test() ~= 1 then error('fail') end")) == "");

    utils::lua::add_arki_global_library(L, "testlib", lib);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("if arki.testlib.test() ~= 1 then error('fail') end")) == "");
});

add_method("arki_types", [] {
    Lua L(true, false);

    types::Type::lua_loadlib(L);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
});

add_method("arki_metadata", [] {
    Lua L(true, false);

    Metadata::lua_openlib(L);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
});

add_method("arki_matcher", [] {
    // Test adding arkimet libraries one at a time
    Lua L(true, false);

    Matcher::lua_openlib(L);
    wassert(actual(lua_gettop(L)) == 0);

    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
});

add_method("arki_code", [] {
    // Test running trivial Lua code, with arkimet libraries loaded
    Lua L(true, true);
    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
});

// Run lua-example/*.lua files, doctest style
add_method("examples", [] {
    ARKI_UTILS_TEST_INFO(tinfo);
    Lua L;

	// Define 'ensure' function
	string ensure = "function ensure(val)\n"
			"  if (not val) then error('assertion failed:\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_equals(a, b)\n"
			"  if (a ~= b) then error('assertion failed: [' .. tostring(a) .. '] ~= [' .. tostring(b) .. ']\\n' .. debug.traceback()) end\n"
			"end\n";
    wassert(actual(L.run_string(ensure)) == "");

    // Run the various lua examples
    string path = "lua-examples";
    sys::Path dir(path);
    for (sys::Path::iterator d = dir.begin(); d != dir.end(); ++d)
    {
        if (!str::endswith(d->d_name, ".lua")) continue;
        string fname = str::joinpath(path, d->d_name);
        tinfo() << "current: " << fname;
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            wassert(actual(error) == "");
        }
    }
});

}

}
