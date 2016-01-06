#include "report.h"
#include "metadata.h"
#include "summary.h"
#include "utils/lua.h"
#include "utils/sys.h"
#include "utils/string.h"
#include "utils/regexp.h"
#include "runtime/config.h"

using namespace std;
using namespace arki::utils;

namespace arki {

Report::Report(const std::string& params) : L(new Lua), m_accepts_metadata(false), m_accepts_summary(false)
{
	if (!params.empty())
		load(params);
}

Report::~Report()
{
	if (L) delete L;
}

void Report::load(const std::string& params)
{
    // Parse params into its components
    Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
    vector<string> args;
    for (Splitter::const_iterator j = sp.begin(params); j != sp.end(); ++j)
        args.push_back(*j);

    if (args.empty())
        throw std::runtime_error("cannot initialize report: report command is empty");

    // Look for the script in all report paths
    loadFile(runtime::Config::get().dir_report.find_file(args[0]));
}

void Report::loadFile(const std::string& fname)
{
    /// Load the report function
    m_filename = fname;

    if (luaL_loadfile(*L, fname.c_str()))
    {
        string msg = "cannot parse Lua code";
        msg += lua_tostring(*L, -1);
        throw std::runtime_error(msg);
    }

    create_lua_object();
}

void Report::loadString(const std::string& buf)
{
    m_filename = "memory buffer";

    if (luaL_loadbuffer(*L, buf.data(), buf.size(), m_filename.c_str()))
    {
        string msg = "cannot parse Lua code: ";
        msg += lua_tostring(*L, -1);
        throw std::runtime_error(msg);
    }

    create_lua_object();
}

void Report::create_lua_object()
{
    if (lua_pcall(*L, 0, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot define report functions: " + error);
    }

    // ensure that there is a Report variable
    lua_getglobal(*L, "Report");
    int type = lua_type(*L, -1);
    if (type == LUA_TNIL)
        throw std::runtime_error("cannot load report code from " + m_filename + ": code did not define a variable called Report");
    if (type != LUA_TTABLE)
        throw std::runtime_error("cannot load report code from " + m_filename + ": the Report variable is not a table");

	// Get the readMetadata function
	lua_pushstring(*L, "readMetadata");
	lua_gettable(*L, -2);
	if (lua_type(*L, -1) == LUA_TNIL)
	{
		m_accepts_metadata = false;
		lua_pop(*L, 1);
	} else {
		m_accepts_metadata = true;
		lua_setglobal(*L, "_md");
	}

	// Get the readSummary function
	lua_pushstring(*L, "readSummary");
	lua_gettable(*L, -2);
	if (lua_type(*L, -1) == LUA_TNIL)
	{
		m_accepts_summary = false;
		lua_pop(*L, 1);
	} else {
		m_accepts_summary = true;
		lua_setglobal(*L, "_su");
	}

    // Get the report function
    lua_pushstring(*L, "report");
    lua_gettable(*L, -2);
    if (lua_type(*L, -1) == LUA_TNIL)
        throw std::runtime_error("cannot load report code from " + m_filename + ": Report.report is nil");
    lua_setglobal(*L, "_re");

    // Pop the Report table from the stack
    lua_pop(*L, 1);

    //utils::lua::dumpstack(L, "Afterinit", stderr);
}

void Report::captureOutput(std::ostream& buf)
{
    utils::lua::capturePrintOutput(*L, buf);
}

void Report::captureOutput(int fd)
{
    utils::lua::capturePrintOutput(*L, fd);
}

bool Report::eat(unique_ptr<Metadata>&& md)
{
    if (!acceptsMetadata()) return true;

    lua_getglobal(*L, "_md");
    lua_getglobal(*L, "Report");
    md->lua_push(*L);
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot process metadata: " + error);
    }
    return true;
}

bool Report::operator()(Summary& s)
{
	if (!acceptsSummary()) return true;

    lua_getglobal(*L, "_su");
    lua_getglobal(*L, "Report");
    s.lua_push(*L);
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot process summary: " + error);
    }
    return true;
}

void Report::report()
{
    lua_getglobal(*L, "_re");
    lua_getglobal(*L, "Report");
    if (lua_pcall(*L, 1, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot generate report: " + error);
    }
}

}
