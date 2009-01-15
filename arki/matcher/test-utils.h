/**
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>

namespace arki {
struct Metadata;

namespace tests {

void fill(Metadata& md);

#define ensure_matches(expr, md) arki::tests::impl_ensure_matches(wibble::tests::Location(__FILE__, __LINE__, #expr ", " #md), (expr), (md), true)
#define ensure_not_matches(expr, md) arki::tests::impl_ensure_matches(wibble::tests::Location(__FILE__, __LINE__, #expr ", " #md), (expr), (md), false)
void impl_ensure_matches(const wibble::tests::Location& loc, const std::string& expr, const Metadata& md, bool shouldMatch = true);

#if 0
#define ensure_serialises(x, y) arki::tests::impl_ensure_serialises(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y))
template<typename T>
static inline void impl_ensure_serialises(const wibble::tests::Location& loc, const arki::Item<T>& o, ser::Code code)
{
    // Binary encoding, without envelope
    std::string enc = o->encodeWithoutEnvelope();
    inner_ensure_equals(T::decode((const unsigned char*)enc.data(), enc.size()), o);

	// Binary encoding, with envelope
    enc = o->encodeWithEnvelope();
    inner_ensure_equals(types::decode((const unsigned char*)enc.data(), enc.size()).upcast<T>(), o);

    const unsigned char* buf = (const unsigned char*)enc.data();
    size_t len = enc.size();
    inner_ensure_equals(types::decodeEnvelope(buf, len), code);
    inner_ensure_equals(o->encodeWithoutEnvelope().size(), len);
    inner_ensure_equals(T::decode(buf, len), o);

	// String encoding
    inner_ensure_equals(T::decodeString(wibble::str::fmt(o)), o);
}
#endif

}
}

#endif
