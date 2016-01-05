#include "arki/wibble/sys/exec.h"
#include "arki/exceptions.h"
#include <string.h>			// strdup
#include <unistd.h>			// execve
#include <malloc.h> // alloca on win32 seems to live there
#include <iostream>

using namespace arki;

extern char **environ;

namespace wibble {
namespace sys {

int Exec::main()
{
	try {
		exec();
	} catch (std::exception& e) {
		std::cerr << e.what() << std::endl;
	}
	return 0;
}

void Exec::importEnv()
{
	for (char** s = environ; *s; ++s)
		env.push_back(*s);
}

void Exec::spawnChild()
{
#ifdef _WIN32
    std::string cmd = pathname;
    for ( int i = 1; i < args.size(); ++i )
        cmd += " \"" + args[i] + "\""; // FIXME: quoting...
    std::cerr << "CreateProcess: " << cmd << std::endl;
    CreateProcess( NULL, (char*)cmd.c_str(), NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi );
#endif
}

void Exec::exec()
{
    // Prepare the argument list
    char** exec_args = new char*[args.size() + 1];
    for (size_t i = 0; i < args.size(); ++i)
        exec_args[i] = strdup(args[i].c_str());
    exec_args[args.size()] = 0;

    char** exec_env = environ;
    if (!envFromParent)
    {
        // Prepare the custom environment
        exec_env = new char*[env.size() + 1];
        for (size_t i = 0; i < env.size(); ++i)
            // We can just store a pointer to the internal strings, since later
            // we're calling exec and no destructors will be called
            exec_env[i] = strdup(env[i].c_str());
        exec_env[env.size()] = 0;
    }

    if (searchInPath)
    {
        if (execvpe(pathname.c_str(), exec_args, exec_env) == -1)
            throw_system_error("trying to run " + pathname);
    } else {
        if (execve(pathname.c_str(), exec_args, exec_env) == -1)
            throw_system_error("trying to run " + pathname);
    }

    delete[] exec_args;
    if (exec_env != environ) delete[] exec_env;
    throw std::runtime_error("cannot run run " + pathname + ": program flow continued after successful exec()");
}

}
}

// vim:set ts=4 sw=4:
