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

class Handler
{
private:
    Handler* orig = nullptr;
    bool installed = false;

public:
    virtual ~Handler();

    virtual void warning(const char* fmt, va_list ap) = 0;
    virtual void verbose(const char* fmt, va_list ap) = 0;
    virtual void debug(const char* fmt, va_list ap) = 0;

    /**
     * Install the handler as the current handler.
     *
     * When the handler is destructed, it will restore the previous one.
     */
    void install();

    /// Format vprintf-style arguments into a std::string
    std::string format(const char* fmt, va_list ap);
};


struct NullHandler: public Handler
{
    void warning(const char* fmt, va_list ap) {}
    void verbose(const char* fmt, va_list ap) {}
    void debug(const char* fmt, va_list ap) {}
};


struct StderrHandler: public Handler
{
    void warning(const char* fmt, va_list ap) override;
    void verbose(const char* fmt, va_list ap) override;
    void debug(const char* fmt, va_list ap) override;
};


/// Collect messages during a test, and print them out during destruction
struct CollectHandler : public Handler
{
    bool _verbose;
    bool _debug;
    std::vector<std::string> collected;

    CollectHandler(bool verbose=true, bool debug=false);
    ~CollectHandler();

    void warning(const char* fmt, va_list ap) override;
    void verbose(const char* fmt, va_list ap) override;
    void debug(const char* fmt, va_list ap) override;

    void collect(const char* fmt, va_list ap);
    void clear();
};

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
void warning(const char* fmt, ...) __attribute__ ((format(printf, 1, 2)));
void warning(const char* fmt, va_list ap);

/// Output a message, if verbose messages are allowed (a newline is automatically appended)
void verbose(const char* fmt, ...) __attribute__ ((format(printf, 1, 2)));
void verbose(const char* fmt, va_list ap);

/// Output a message, if debug messages are allowed (a newline is automatically appended)
void debug(const char* fmt, ...) __attribute__ ((format(printf, 1, 2)));
void debug(const char* fmt, va_list ap);

/// Output a message to /dev/tty (used for debugging when the output is redirected)
void debug_tty(const char* fmt, ...) __attribute__ ((format(printf, 1, 2)));

}
}

#endif
