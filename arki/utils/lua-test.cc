#include "arki/tests/tests.h"
#include "arki/utils/lua.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

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

add_method("arki_code", [] {
    // Test running trivial Lua code, with arkimet libraries loaded
    Lua L(true, true);
    wassert(actual(L.run_string("a=3")) == "");
    wassert(actual(L.run_string("foo(")).matches("unexpected symbol near"));
});

}

}
