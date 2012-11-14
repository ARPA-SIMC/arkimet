/**
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace wibble;

namespace arki {
namespace tests {

Location::Location(const Location& parent, const std::string& file, int line)
    : parent(&parent), file(file), line(line)
{
}

void Location::backtrace(std::ostream& out) const
{
    if (parent) parent->backtrace(out);
    if (file.empty()) return;
    out << file << ":" << line << endl;
}

void Location::fail_test(const std::string& msg)
{
    std::stringstream ss;
    backtrace(ss);
    ss << msg << endl;
    throw tut::failure(ss.str());
}

void impl_ensure_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle)
{
    if( haystack.find(needle) == std::string::npos )
    {
        std::stringstream ss;
        ss << "'" << haystack << "' does not contain '" << needle << "'";
        throw tut::failure(loc.msg(ss.str()));
    }
}

void impl_ensure_not_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle)
{
    if( haystack.find(needle) != std::string::npos )
    {
        std::stringstream ss;
        ss << "'" << haystack << "' must not contain '" << needle << "'";
        throw tut::failure(loc.msg(ss.str()));
    }
}

void ensure_file_exists(LOCPRM, const std::string& fname)
{
    if (not sys::fs::exists(fname))
    {
        std::stringstream ss;
        ss << "file '" << fname << "' does not exists";
        throw tut::failure(loc.msg(ss.str()));
    }
}

void ensure_not_file_exists(LOCPRM, const std::string& fname)
{
    if (sys::fs::exists(fname))
    {
        std::stringstream ss;
        ss << "file '" << fname << "' does exists";
        throw tut::failure(loc.msg(ss.str()));
    }
}
}
}
// vim:set ts=4 sw=4:
