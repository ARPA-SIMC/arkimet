#include <arki/tests/lua.h>
#include <arki/utils/lua.h>
#include <wibble/exception.h>

using namespace std;
using namespace arki;

namespace arki {
namespace tests {

Lua::Lua(const std::string& src) : L(0), arg_count(0)
{
	// Initialise the lua logic
	L = lua_open();

	// NOTE: This one is optional: only use it for debugging
	#if LUA_VERSION_NUM >= 501
	luaL_openlibs(L);
	#else
	luaopen_base(L);              /* opens the basic library */
	luaopen_table(L);             /* opens the table library */
	luaopen_io(L);                /* opens the I/O library */
	luaopen_string(L);            /* opens the string lib. */
	luaopen_math(L);              /* opens the math lib. */
	luaopen_loadlib(L);           /* loadlib function */
	luaopen_debug(L);             /* debug library  */
	lua_settop(L, 0);
	#endif

	if (!src.empty())
		loadString(src);
}

Lua::~Lua()
{
	if (L) lua_close(L);
}

/// Load the test code from the given file
void Lua::loadFile(const std::string& fname)
{
	/// Load the prettyprinting functions
	m_filename = fname;

	if (luaL_loadfile(L, fname.c_str()))
		throw wibble::exception::Consistency("parsing Lua code", lua_tostring(L, -1));

	create_lua_object();
}

/// Load the test code from the given string containing Lua source code
void Lua::loadString(const std::string& buf)
{
	m_filename = "memory buffer";

	if (luaL_loadbuffer(L, buf.data(), buf.size(), m_filename.c_str()))
		throw wibble::exception::Consistency("parsing Lua code", lua_tostring(L, -1));

	create_lua_object();
}

/// Runs the parsed code to let it define the 'test' function we are going
/// to use
void Lua::create_lua_object()
{
	if (lua_pcall(L, 0, 0, 0))
	{
		string error = lua_tostring(L, -1);
		lua_pop(L, 1);
		throw wibble::exception::Consistency(error, "defining pretty printing functions");
	}

	// ensure that there is a 'test' function
	lua_getglobal(L, "test");
	int type = lua_type(L, -1);
	if (type == LUA_TNIL)
		throw wibble::exception::Consistency(
			"loading test code from " + m_filename,
			"code did not define a function called 'test'");
	if (type != LUA_TFUNCTION)
		throw wibble::exception::Consistency(
			"loading report code from " + m_filename,
			"the 'test' variable is not a function");

	// Pop the 'test' function from the stack
	lua_pop(L, 1);
}
		
void Lua::captureOutput(std::ostream& buf)
{
	utils::lua::capturePrintOutput(L, buf);
}

std::string Lua::run()
{
	// No arguments have been pushed, then at least push the function
	if (arg_count == 0)
		lua_getglobal(L, "test");

	if (lua_pcall(L, arg_count, 1, 0))
	{
		string error = lua_tostring(L, -1);
		lua_pop(L, 1);
		throw wibble::exception::Consistency(error, "calling test function");
	}
	arg_count = 0;
	const char* res = lua_tostring(L, -1);
	lua_pop(L, 1);
	return res == NULL ? string() : res;
}

}
}
// vim:set ts=4 sw=4:
