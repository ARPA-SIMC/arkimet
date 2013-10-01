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
	Item<Source> o = source::Blob::create("test", "", "testfile", 21, 42);
	ensure_equals(o->style(), Source::BLOB);
	const source::Blob* v = o->upcast<source::Blob>();
	ensure_equals(v->format, "test");
	ensure_equals(v->filename, "testfile");
	ensure_equals(v->offset, 21u);
	ensure_equals(v->size, 42u);

	#if 0
	source.prependPath("/pippo");
	source.getBlob(fname, offset, size);
	ensure_equals(fname, "/pippo/testfile");
	ensure_equals(offset, 21u);
	ensure_equals(size, 42u);
	#endif

	ensure_equals(o, Item<Source>(source::Blob::create("test", "", "testfile", 21, 42)));

	ensure(o != source::Blob::create("test", "", "testfile", 22, 43));
	ensure(o != source::URL::create("test", "testfile"));
	ensure(o != source::Inline::create("test", 43));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check URL
template<> template<>
void to::test<2>()
{
	Item<Source> o = source::URL::create("test", "http://foobar");
	ensure_equals(o->style(), Source::URL);
	const source::URL* v = o->upcast<source::URL>();
	ensure_equals(v->format, "test");
	ensure_equals(v->url, "http://foobar");

	ensure_equals(o, Item<Source>(source::URL::create("test", "http://foobar")));

	ensure(o != source::URL::create("test", "http://foobar/a"));
	ensure(o != source::Blob::create("test", "", "http://foobar", 1, 2));
	ensure(o != source::Inline::create("test", 1));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Inline
template<> template<>
void to::test<3>()
{
	Item<Source> o = source::Inline::create("test", 12345);
	ensure_equals(o->style(), Source::INLINE);
	const source::Inline* v = o->upcast<source::Inline>();
	ensure_equals(v->format, "test");
	ensure_equals(v->size, 12345u);

	ensure_equals(o, Item<Source>(source::Inline::create("test", 12345)));

	ensure(o != source::Inline::create("test", 12344));
	ensure(o != source::Blob::create("test", "", "", 0, 12344));
	ensure(o != source::URL::create("test", ""));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob on big files
template<> template<>
void to::test<4>()
{
	Item<Source> o = source::Blob::create("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42);
	ensure_equals(o->style(), Source::BLOB);
	const source::Blob* v = o->upcast<source::Blob>();
	ensure_equals(v->format, "test");
	ensure_equals(v->filename, "testfile");
	ensure_equals(v->offset, 0x8000FFFFffffFFFFLLU);
	ensure_equals(v->size, 42u);

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob and pathnames handling
template<> template<>
void to::test<5>()
{
    Item<source::Blob> o = source::Blob::create("test", "", "testfile", 21, 42);
    ensure_equals(o->absolutePathname(), "testfile");

    o = source::Blob::create("test", "/tmp", "testfile", 21, 42);
    ensure_equals(o->absolutePathname(), "/tmp/testfile");

    o = source::Blob::create("test", "/tmp", "/antani/testfile", 21, 42);
    ensure_equals(o->absolutePathname(), "/antani/testfile");
}

// Check Blob and pathnames handling in serialization
template<> template<>
void to::test<6>()
{
    using namespace types::source;

    Item<Blob> o = source::Blob::create("test", "/tmp", "testfile", 21, 42);

    // Encode to binary, decode, we lose the root
    string enc = o->encodeWithEnvelope();
    Item<Blob> decoded = types::decode((const unsigned char*)enc.data(), enc.size()).upcast<Blob>();

    ensure_equals(decoded->basedir, string());
    ensure_equals(decoded->filename, "testfile");

    // Encode to YAML, decode, we keep the root
    enc = wibble::str::fmt(o);
    decoded = types::Source::decodeString(enc).upcast<Blob>();
    ensure_equals(decoded->basedir, string());
    ensure_equals(decoded->filename, "/tmp/testfile");

    // Encode to JSON, decode, we keep the root
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        o->serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);

        decoded = types::decodeMapping(parsed.root().get_mapping()).upcast<Blob>();
        ensure_equals(decoded->basedir, string("/tmp"));
        ensure_equals(decoded->filename, "testfile");
    }
}

}

// vim:set ts=4 sw=4:
