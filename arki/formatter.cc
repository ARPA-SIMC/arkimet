/*
 * formatter - Arkimet prettyprinter
 *
 * Copyright (C) 2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/formatter.h>
#include <arki/types.h>
#include <arki/formatter/lua.h>
#include <wibble/string.h>
#include "config.h"

using namespace std;

namespace arki {

Formatter::Formatter() {}
Formatter::~Formatter() {}
string Formatter::operator()(const Item<>& v) const { return wibble::str::fmt(v); }

Formatter* Formatter::create()
{
#if HAVE_LUA
	return new formatter::Lua;
#else
	return new Formatter;
#endif
}

}
// vim:set ts=4 sw=4:
