/*
 * nag - Verbose and debug output support
 *
 * Copyright (C) 2005--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#ifndef ARKI_NAG_H
#define ARKI_NAG_H

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

}
}

#endif
