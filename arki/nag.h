#ifndef ARKI_NAG_H
#define ARKI_NAG_H

#include <vector>
#include <string>
#include <cstdarg>

/** @file
 * Debugging aid framework that allows to print, at user request, runtime
 * verbose messages about internal status and operation.
 */

namespace arki {
namespace nag {

/**
 * Initialize the verbose printing interface, taking the allowed verbose level
 * from the environment and printing a little informational banner if any
 * level of verbose messages are enabled.
 */
void init(bool verbose=false, bool debug=false, bool testing=false);

/// Check if verbose output is enabled
bool is_verbose();

/// Check if debug output is enabled
bool is_debug();

/// Output a message, except during tests (a newline is automatically appended)
void warning(const char* fmt, ...);

/// Output a message, if verbose messages are allowed (a newline is automatically appended)
void verbose(const char* fmt, ...);

/// Output a message, if debug messages are allowed (a newline is automatically appended)
void debug(const char* fmt, ...);

/// Collect messages during a test, and print them out during destruction
struct TestCollect
{
    TestCollect* previous_collector;
    bool verbose;
    bool debug;
    std::vector<std::string> collected;

    TestCollect(bool verbose=true, bool debug=false);
    ~TestCollect();

    void collect(const char* fmt, va_list ap);
    void clear();
};

}
}

#endif
