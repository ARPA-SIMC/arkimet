/*
 * arki-shell - Interactive arkimet Lua shell
 *
 * Copyright (C) 2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <arki/utils/lua.h>
#include <arki/runtime.h>

#include "config.h"

#ifdef HAVE_LIBREADLINE
 #if defined(HAVE_READLINE_READLINE_H)
  #include <readline/readline.h>
 #elif defined(HAVE_READLINE_H)
  #include <readline.h>
 #else /* !defined(HAVE_READLINE_H) */
extern "C" {
  extern char *readline (const char* prompt);
  extern int rl_insert_text (const char *text);
  typedef int (rl_hook_func_t)(void);
  extern rl_hook_func_t * rl_startup_hook;
}
 #endif /* !defined(HAVE_READLINE_H) */
 char *cmdline = NULL;
#else /* !defined(HAVE_READLINE_READLINE_H) */
 /* no readline */
#endif /* HAVE_LIBREADLINE */


using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::sys;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
//    VectorOption<String>* cfgfiles;
//    BoolOption* fix;
//    BoolOption* repack;
//    BoolOption* invalidate;
//    BoolOption* stats;
//    StringOption* salvage;
//    StringOption* op_remove;

    Options() : StandardParserWithManpage("arki-shell", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[script]";
        description = "Interactive Lua shell with Arkimet functions.";
    }
};

}
}

// Read a command from the user
string read_command(const std::string& prompt)
{
    char* rl_answer = readline(prompt.c_str());
    if (!rl_answer) return "\\q";
    string answer = rl_answer;
    free(rl_answer);
    return str::trim(answer);
}

struct Interpreter : public Lua
{
    // Load a file as a function on top of the stack
    void load_file(const std::string& fname)
    {
        if (luaL_loadfile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw wibble::exception::Consistency("parsing Lua code from " + fname, error);
        }
    }

    void load_string(const std::string& buf)
    {
        if (luaL_loadbuffer(L, buf.data(), buf.size(), "(shell input)"))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw wibble::exception::Consistency("parsing Lua code", error);
        }
    }

    // Run the code that has just been loaded as a function on top of the stack
    void run_code()
    {
        if (lua_pcall(L, 0, 0, 0))
        {
            string error = lua_tostring(L, -1);
            lua_pop(L, 1);
            throw wibble::exception::Consistency("running code", error);
        }
    }
};


struct Shell
{
    Interpreter& interpreter;

    Shell(Interpreter& interpreter) : interpreter(interpreter) {}

    void main_loop()
    {
        while (true)
        {
            string cmd = read_command("> ");
            if (cmd == "?")
                run_help();
            else if (cmd == "\\q" || cmd == "quit")
                return;
            else
                run_command(cmd);
        }
    }

    void run_command(const std::string& cmd)
    {
        try {
            interpreter.load_string(cmd);
        } catch (std::exception& e) {
            cerr << e.what() << endl;
            return;
        }

        try {
            interpreter.run_code();
        } catch (std::exception& e) {
            cerr << e.what() << endl;
        }
    }

    void run_help()
    {
        cout << "Enter a Lua statement to execute it." << endl
             << "Special commands:" << endl
             << " ?         prints this help message" << endl
             << " \\q, quit  quits the shell" << endl
        ;
    }
};


int main(int argc, const char* argv[])
{
    wibble::commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;
        runtime::init();

        Interpreter interpreter;

        if (opts.hasNext())
        {
            // do script
            interpreter.load_file(opts.next());
            interpreter.run_code();
        } else {
            // do interactive
            Shell shell(interpreter);
            shell.main_loop();
        }
    } catch (wibble::exception::BadOption& e) {
        cerr << e.desc() << endl;
        opts.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }

    return 0;
}

// vim:set ts=4 sw=4:
