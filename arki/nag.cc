/*
 * nag - Verbose and debug output support
 *
 * Copyright (C) 2005--2011  ARPAE-SIMC <simc-urp@arpae.it>
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

#include "config.h"

#include <arki/nag.h>

#include <cstdlib>
#include <cstdio>
#include <cstdarg>

#ifdef __DATE__
#define DATE __DATE__
#else
#define DATE "??? ?? ????"
#endif

#ifdef __TIME__
#define TIME __TIME__
#else
#define TIME "??:??:??"
#endif

using namespace std;

namespace arki {
namespace nag {

static bool _verbose = false;
static bool _debug = false;
static bool _testing = false;
static TestCollect* collector = 0;

void init(bool verbose, bool debug, bool testing)
{
	_testing = testing;
	_verbose = verbose;
	if (debug)
		_debug = _verbose = true;

	if (_verbose)
	{
		fprintf(stderr, "%s\n", PACKAGE_NAME " " PACKAGE_VERSION ", compiled on " DATE " " TIME);
		fprintf(stderr, "%s",
			"Copyright (C) 2007-2010 ARPA Emilia Romagna.\n"
			PACKAGE_NAME " comes with ABSOLUTELY NO WARRANTY.\n"
			"This is free software, and you are welcome to redistribute it and/or modify it\n"
			"under the terms of the GNU General Public License as published by the Free\n"
			"Software Foundation; either version 2 of the License, or (at your option) any\n"
			"later version.\n");
	}
}

bool is_verbose() { return _verbose; }
bool is_debug() { return _debug; }

void warning(const char* fmt, ...)
{
    if (collector)
    {
        va_list ap;
        va_start(ap, fmt);
        collector->collect(fmt, ap);
        va_end(ap);
        return;
    }

	if (_testing) return;

	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	putc('\n', stderr);
}

void verbose(const char* fmt, ...)
{
    if (collector && collector->verbose)
    {
        va_list ap;
        va_start(ap, fmt);
        collector->collect(fmt, ap);
        va_end(ap);
        return;
    }

	if (!_verbose) return;

	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	putc('\n', stderr);
}

void debug(const char* fmt, ...)
{
    if (collector && collector->debug)
    {
        va_list ap;
        va_start(ap, fmt);
        collector->collect(fmt, ap);
        va_end(ap);
        return;
    }

	if (!_debug) return;

	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	putc('\n', stderr);
}

TestCollect::TestCollect(bool verbose, bool debug)
    : previous_collector(collector), verbose(verbose), debug(debug)
{
    collector = this;
}

TestCollect::~TestCollect()
{
    collector = previous_collector;
    for (vector<string>::const_iterator i = collected.begin();
            i != collected.end(); ++i)
    {
        fwrite(i->data(), i->size(), 1, stderr);
        putc('\n', stderr);
    }
}

void TestCollect::collect(const char* fmt, va_list ap)
{
    char buf[1024];
    int size = vsnprintf(buf, 1024, fmt, ap);
    if (size > 1024)
        size = 1024;
    collected.push_back(string(buf, size));
}

void TestCollect::clear()
{
    collected.clear();
}

}
}
