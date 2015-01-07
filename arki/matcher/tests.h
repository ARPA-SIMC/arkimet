/**
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_MATCHER_TESTUTILS_H
#define ARKI_MATCHER_TESTUTILS_H

#include <arki/metadata/tests.h>

namespace arki {
struct Metadata;

namespace tests {

void fill(Metadata& md);

#define ensure_matches(expr, md) arki::tests::impl_ensure_matches(wibble::tests::Location(__FILE__, __LINE__, #expr ", " #md), (expr), (md), true)
#define ensure_not_matches(expr, md) arki::tests::impl_ensure_matches(wibble::tests::Location(__FILE__, __LINE__, #expr ", " #md), (expr), (md), false)
void impl_ensure_matches(const wibble::tests::Location& loc, const std::string& expr, const Metadata& md, bool shouldMatch = true);

}
}

#endif
