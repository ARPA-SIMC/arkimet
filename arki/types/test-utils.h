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
#ifndef ARKI_TYPES_TESTUTILS_H
#define ARKI_TYPES_TESTUTILS_H

#include <arki/tests/test-utils.h>
#include <arki/types.h>
#include <arki/utils/codec.h>
#include <wibble/string.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

namespace arki {
namespace tests {

#define ensure_serialises(x, y) arki::tests::impl_ensure_serialises(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y))
template<typename T>
static inline void impl_ensure_serialises(const wibble::tests::Location& loc, const arki::Item<T>& o, types::Code code)
{
    // Binary encoding, without envelope
    std::string enc;
    utils::codec::Encoder e(enc);
    o->encodeWithoutEnvelope(e);
    inner_ensure_equals(arki::Item<T>(T::decode((const unsigned char*)enc.data(), enc.size())), o);

    // Binary encoding, with envelope
    enc = o->encodeWithEnvelope();
//  Rewritten in the next two lines due to, it seems, a bug in old gccs
//  inner_ensure_equals(types::decode((const unsigned char*)enc.data(), enc.size()).upcast<T>(), o);
    Item<> decoded = types::decode((const unsigned char*)enc.data(), enc.size());
    inner_ensure_equals(decoded, Item<>(o));


    const unsigned char* buf = (const unsigned char*)enc.data();
    size_t len = enc.size();
    inner_ensure_equals(types::decodeEnvelope(buf, len), code);
    {
        std::string oenc;
        utils::codec::Encoder oe(oenc);
        o->encodeWithoutEnvelope(oe);
        inner_ensure_equals(oenc.size(), len);
    }
    inner_ensure_equals(arki::Item<T>(T::decode(buf, len)), o);

    // String encoding
    inner_ensure_equals(arki::Item<T>(T::decodeString(wibble::str::fmt(o))), o);

    // JSON encoding
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        o->serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);
        inner_ensure(parsed.root().is_mapping());
        arki::Item<> iparsed = types::decodeMapping(parsed.root().get_mapping());
        inner_ensure_equals(o, iparsed);
    }
}

#define ensure_compares(x, y, z) arki::tests::impl_ensure_compares(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y ", " #z), (x), (y), (z))
template<typename T>
static inline void impl_ensure_compares(const wibble::tests::Location& loc, const arki::Item<T>& x, const arki::Item<T>& y, const arki::Item<T>& z)
{
    inner_ensure(x == x);
    inner_ensure(y == y);
    inner_ensure(z == z);

    inner_ensure(x < y);
    inner_ensure(x <= y);
    inner_ensure(not (x == y));
    inner_ensure(x != y);
    inner_ensure(not (x >= y));
    inner_ensure(not (x > y));

    inner_ensure(not (y < z));
    inner_ensure(y <= z);
    inner_ensure(y == z);
    inner_ensure(not (y != z));
    inner_ensure(y >= z);
    inner_ensure(not (y > z));
}

void test_assert_sourceblob_is(LOCPRM,
        const std::string& format,
        const std::string& basedir,
        const std::string& fname,
        uint64_t ofs,
        uint64_t size,
        const arki::Item<>& item);

}
}

#endif
