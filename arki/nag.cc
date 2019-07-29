#include "nag.h"
#include <cstdio>
#include <cstdarg>
#include <stdexcept>

using namespace std;

namespace arki {
namespace nag {

static bool _verbose = false;
static bool _debug = false;
static Handler* handler = nullptr;


Handler::~Handler()
{
    if (installed)
        handler = orig;
}

void Handler::install()
{
    if (installed)
        throw std::runtime_error("Cannot install the same nag handler twice");
    orig = handler;
    handler = this;
    installed = true;
}

std::string Handler::format(const char* fmt, va_list ap)
{
    // Use a copy of ap to compute the size, since a va_list can be iterated
    // only once
    va_list ap1;
    va_copy(ap1, ap);
    auto size = vsnprintf(nullptr, 0, fmt, ap1);
    va_end(ap1);

    std::string res(size + 1, '\0');
    // TODO: remove the const cast when we have C++17
    vsnprintf(const_cast<char*>(res.data()), size + 1, fmt, ap);
    res.resize(size);

    return res;
}


void StderrHandler::warning(const char* fmt, va_list ap)
{
    vfprintf(stderr, fmt, ap);
    putc('\n', stderr);
}

void StderrHandler::verbose(const char* fmt, va_list ap)
{
    vfprintf(stderr, fmt, ap);
    putc('\n', stderr);
}

void StderrHandler::debug(const char* fmt, va_list ap)
{
    vfprintf(stderr, fmt, ap);
    putc('\n', stderr);
}


CollectHandler::CollectHandler(bool verbose, bool debug)
    : _verbose(verbose), _debug(debug)
{
}

CollectHandler::~CollectHandler()
{
    for (const auto& str: collected)
    {
        fwrite(str.data(), str.size(), 1, stderr);
        putc('\n', stderr);
    }
}

void CollectHandler::clear()
{
    collected.clear();
}

void CollectHandler::warning(const char* fmt, va_list ap)
{
    collected.push_back("W:" + format(fmt, ap));
}

void CollectHandler::verbose(const char* fmt, va_list ap)
{
    if (!_verbose)
        return;
    collected.push_back("V:" + format(fmt, ap));
}

void CollectHandler::debug(const char* fmt, va_list ap)
{
    if (!_debug)
        return;
    collected.push_back("D:" + format(fmt, ap));
}


void init(bool verbose, bool debug, bool testing)
{
    if (testing)
    {
        _verbose = true;
        _debug = true;
    } else {
        _verbose = verbose;
        if (debug)
            _debug = _verbose = true;

        if (!handler)
            handler = new StderrHandler;
    }
}

bool is_verbose() { return _verbose; }
bool is_debug() { return _debug; }


void warning(const char* fmt, ...)
{
    if (!handler) return;

    va_list ap;
    va_start(ap, fmt);
    try {
        handler->warning(fmt, ap);
    } catch (...) {
        va_end(ap);
        throw;
    }
    va_end(ap);
}

void verbose(const char* fmt, ...)
{
    if (!_verbose || !handler) return;

    va_list ap;
    va_start(ap, fmt);
    try {
        handler->verbose(fmt, ap);
    } catch (...) {
        va_end(ap);
        throw;
    }
    va_end(ap);
}

void debug(const char* fmt, ...)
{
    if (!_debug || !handler) return;

    va_list ap;
    va_start(ap, fmt);
    try {
        handler->debug(fmt, ap);
    } catch (...) {
        va_end(ap);
        throw;
    }
    va_end(ap);
}


}
}
