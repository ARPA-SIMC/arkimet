/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/tests.h>
#include <arki/types/source.h>
#include <arki/types/source/blob.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_source_shar {
};
TESTGRP(arki_types_source);

// Check Blob
template<> template<>
void to::test<1>()
{
    auto_ptr<Source> o = Source::createBlob("test", "", "testfile", 21, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 21u, 42u));

	#if 0
	source.prependPath("/pippo");
	source.getBlob(fname, offset, size);
	ensure_equals(fname, "/pippo/testfile");
	ensure_equals(offset, 21u);
	ensure_equals(size, 42u);
	#endif

    wassert(actual(o) == Source::createBlob("test", "", "testfile", 21, 42));
    wassert(actual(o) == Source::createBlob("test", "/tmp", "testfile", 21, 42));
    wassert(actual(o) != Source::createBlob("test", "", "testfile", 22, 43));
    wassert(actual(o) != Source::createURL("test", "testfile"));
    wassert(actual(o) != Source::createInline("test", 43));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check URL
template<> template<>
void to::test<2>()
{
    auto_ptr<Source> o = Source::createURL("test", "http://foobar");
    wassert(actual(o).is_source_url("test", "http://foobar"));

    wassert(actual(o) == Source::createURL("test", "http://foobar"));
    wassert(actual(o) != Source::createURL("test", "http://foobar/a"));
    wassert(actual(o) != Source::createBlob("test", "", "http://foobar", 1, 2));
    wassert(actual(o) != Source::createInline("test", 1));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Inline
template<> template<>
void to::test<3>()
{
    auto_ptr<Source> o = Source::createInline("test", 12345);
    wassert(actual(o).is_source_inline("test", 12345u));

    wassert(actual(o) == Source::createInline("test", 12345));
    wassert(actual(o) != Source::createInline("test", 12344));
    wassert(actual(o) != Source::createBlob("test", "", "", 0, 12344));
    wassert(actual(o) != Source::createURL("test", ""));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob on big files
template<> template<>
void to::test<4>()
{
    auto_ptr<Source> o = Source::createBlob("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42u));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob and pathnames handling
template<> template<>
void to::test<5>()
{
    auto_ptr<source::Blob> o = source::Blob::create("test", "", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "testfile");

    o = source::Blob::create("test", "/tmp", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/tmp/testfile");

    o = source::Blob::create("test", "/tmp", "/antani/testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/antani/testfile");
}

// Check Blob and pathnames handling in serialization
template<> template<>
void to::test<6>()
{
    auto_ptr<Source> o = Source::createBlob("test", "/tmp", "testfile", 21, 42);

    // Encode to binary, decode, we lose the root
    string enc = o->encodeBinary();
    auto_ptr<Source> decoded = downcast<Source>(types::decode((const unsigned char*)enc.data(), enc.size()));
    wassert(actual(decoded).is_source_blob("test", "", "testfile", 21, 42));

    // Encode to YAML, decode, we keep the root
    enc = wibble::str::fmt(*o);
    decoded = types::Source::decodeString(enc);
    wassert(actual(decoded).is_source_blob("test", "/tmp", "testfile", 21, 42));

    // Encode to JSON, decode, we keep the root
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        o->serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);

        decoded = downcast<Source>(types::decodeMapping(parsed.root().get_mapping()));
        wassert(actual(decoded).is_source_blob("test", "/tmp", "testfile", 21, 42));
    }
}

// Test basic type ops
template<> template<>
void to::test<7>()
{
    arki::tests::TestGenericType t("source", "INLINE(bufr,10)");
    t.lower.push_back("INLINE(aaa, 11)");
    t.lower.push_back("INLINE(bufr, 9)");
    t.higher.push_back("INLINE(bufr, 11)");
    t.higher.push_back("INLINE(ccc, 9)");
    wassert(t);
}

}
