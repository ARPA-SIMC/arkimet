/*
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/tests.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/source/blob.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

#include <sstream>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
	return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_metadata_shar {
	Metadata md;
	ValueBag testValues;

	arki_metadata_shar()
	{
		testValues.set("foo", Value::createInteger(5));
		testValues.set("bar", Value::createInteger(5000));
		testValues.set("baz", Value::createInteger(-200));
		testValues.set("moo", Value::createInteger(0x5ffffff));
		testValues.set("antani", Value::createInteger(-1));
		testValues.set("blinda", Value::createInteger(0));
		testValues.set("supercazzola", Value::createInteger(-1234567));
		testValues.set("pippo", Value::createString("pippo"));
		testValues.set("pluto", Value::createString("12"));
		testValues.set("cippo", Value::createString(""));
	}

	void fill(Metadata& md)
	{
		md.set(origin::GRIB1::create(1, 2, 3));
		md.set(product::GRIB1::create(1, 2, 3));
		md.set(level::GRIB1::create(114, 12, 34));
		md.set(timerange::GRIB1::create(1, 1, 2, 3));
		md.set(area::GRIB::create(testValues));
		md.set(proddef::GRIB::create(testValues));
		md.add_note(types::Note::create("test note"));
		md.set(AssignedDataset::create("dsname", "dsid"));
		// Test POSITION reference times
		md.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
	}

#define ensure_matches_fill(x) impl_ensure_matches_fill(wibble::tests::Location(__FILE__, __LINE__, #x), (x))
	void impl_ensure_matches_fill(const wibble::tests::Location& loc, Metadata& md1)
	{
		inner_ensure(md1.get(types::TYPE_ORIGIN).defined());
		inner_ensure_equals(md1.get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(1, 2, 3)));
		inner_ensure(md1.get(types::TYPE_PRODUCT).defined());
		inner_ensure_equals(md1.get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(1, 2, 3)));
		inner_ensure(md1.get(types::TYPE_LEVEL).defined());
		inner_ensure_equals(md1.get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(114, 12, 34)));
		inner_ensure(md1.get(types::TYPE_TIMERANGE).defined());
		inner_ensure_equals(md1.get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(1, 1, 2, 3)));
		inner_ensure(md1.get(types::TYPE_AREA).defined());
		inner_ensure_equals(md1.get(types::TYPE_AREA), Item<>(area::GRIB::create(testValues)));
		inner_ensure(md1.get(types::TYPE_PRODDEF).defined());
		inner_ensure_equals(md1.get(types::TYPE_PRODDEF), Item<>(proddef::GRIB::create(testValues)));
		inner_ensure_equals(md1.notes().size(), 1u);
		inner_ensure_equals((*md1.notes().begin())->content, "test note");
		inner_ensure_equals(md1.get(types::TYPE_ASSIGNEDDATASET), Item<>(AssignedDataset::create("dsname", "dsid")));
		inner_ensure(md1.get(types::TYPE_REFTIME).defined());
		inner_ensure_equals(md1.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1))));
	}
};
TESTGRP(arki_metadata);


// Test sources
template<> template<>
void to::test<1>()
{
    md.source = Source::createBlob("grib", "", "fname", 1, 2);
	ensure_equals(md.source->style(), Source::BLOB);
	ensure_equals(md.source->format, "grib");

	UItem<source::Blob> blob = md.source.upcast<source::Blob>();
	ensure_equals(blob->filename, "fname");
	ensure_equals(blob->offset, 1u);
	ensure_equals(blob->size, 2u);
}

#if 0
static void dump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", *i);
	}
	fprintf(stderr, "\n");
}
#endif

// Test encoding and decoding
template<> template<>
void to::test<2>()
{
	md.source = Source::createBlob("grib", "", "fname", 1, 2);
	fill(md);

	string encoded = md.encode();
	stringstream stream(encoded, ios_base::in);

	Metadata md1;
	md1.read(stream, "(test memory buffer)");

    ensure_equals(md1.source, Source::createBlob("grib", "", "fname", 1, 2));
	ensure_equals(md1.source->format, "grib");
	ensure_matches_fill(md1);


	// Test PERIOD reference times
	md.set(reftime::Period::create(
		types::Time::create(2007, 6, 5, 4, 3, 2),
		types::Time::create(2008, 7, 6, 5, 4, 3)));

	encoded = md.encode();
	stringstream stream1(encoded, ios_base::in);

	Metadata md2;
	md2.read(stream1, "(test memory buffer)");

	ensure(md2.get(types::TYPE_REFTIME).defined());
	ensure_equals(md2.get(types::TYPE_REFTIME),
		Item<>(reftime::Period::create(
			types::Time::create(2007, 6, 5, 4, 3, 2),
			types::Time::create(2008, 7, 6, 5, 4, 3))));
}

// Test Yaml encoding and decoding
template<> template<>
void to::test<3>()
{
    md.source = Source::createBlob("grib", "", "fname", 1, 2);
    fill(md);

	stringstream output;
	md.writeYaml(output);
	stringstream stream(output.str(), ios_base::in);

	Metadata md1;
	md1.readYaml(stream, "(test memory buffer)");

    ensure_equals(md1.source, Source::createBlob("grib", "", "fname", 1, 2));
	ensure_equals(md1.source->format, "grib");
	ensure_matches_fill(md1);


	// Test PERIOD reference times
	md.set(reftime::Period::create(
		types::Time::create(2007, 6, 5, 4, 3, 2),
		types::Time::create(2008, 7, 6, 5, 4, 3)));

	stringstream output1;
	md.writeYaml(output1);
	stringstream stream1(output1.str(), ios_base::in);

	Metadata md2;
	md2.readYaml(stream1, "(test memory buffer)");

	ensure(md2.get(types::TYPE_REFTIME).defined());
	ensure_equals(md2.get(types::TYPE_REFTIME),
		Item<>(reftime::Period::create(
			types::Time::create(2007, 6, 5, 4, 3, 2),
			types::Time::create(2008, 7, 6, 5, 4, 3))));
}

// Test JSON encoding and decoding
template<> template<>
void to::test<4>()
{
    md.source = Source::createBlob("grib", "", "fname", 1, 2);
    fill(md);

    // Serialise to JSON;
    stringstream output;
    emitter::JSON json(output);
    md.serialise(json);

    // Parse back
    stringstream stream(output.str(), ios_base::in);
    emitter::Memory parsed;
    emitter::JSON::parse(stream, parsed);

    Metadata md1;
    md1.read(parsed.root().want_mapping("parsing metadata"));

    ensure_equals(md1.source, Source::createBlob("grib", "", "fname", 1, 2));
    ensure_equals(md1.source->format, "grib");
    ensure_matches_fill(md1);


    // Test PERIOD reference times
    md.set(reftime::Period::create(
                types::Time::create(2007, 6, 5, 4, 3, 2),
                types::Time::create(2008, 7, 6, 5, 4, 3)));

    // Serialise to JSON
    stringstream output1;
    emitter::JSON json1(output1);
    md.serialise(json1);

    // Parse back
    stringstream stream1(output1.str(), ios_base::in);
    emitter::Memory parsed1;
    emitter::JSON::parse(stream1, parsed1);

    Metadata md2;
    md2.read(parsed1.root().want_mapping("parsing metadata"));

    ensure(md2.get(types::TYPE_REFTIME).defined());
    ensure_equals(md2.get(types::TYPE_REFTIME),
            Item<>(reftime::Period::create(
                    types::Time::create(2007, 6, 5, 4, 3, 2),
                    types::Time::create(2008, 7, 6, 5, 4, 3))));
}

// Test encoding and decoding with inline data
template<> template<>
void to::test<5>()
{
	// Here is some data
	wibble::sys::Buffer buf(4);
	((char*)buf.data())[0] = 'c';
	((char*)buf.data())[1] = 'i';
	((char*)buf.data())[2] = 'a';
	((char*)buf.data())[3] = 'o';

	md.setInlineData("test", buf);

	// Encode
	stringstream output;
	md.write(output, "(test memory buffer)");

	// Decode
	stringstream input(output.str(), ios_base::in);
	Metadata md1;
	md1.read(input, "(test memory buffer)");

	ensure(md1.getData() == buf);
}

// Ensure that serialisation to binary preserves the deleted flag
template<> template<>
void to::test<6>()
{
	// Skip: there is no deleted flag anymore
}

// Test Lua functions
template<> template<>
void to::test<7>()
{
#ifdef HAVE_LUA
    md.source = Source::createBlob("grib", "", "fname", 1, 2);
    fill(md);

	tests::Lua test(
		"function test(md) \n"
		"  if md.source == nil then return 'source is nil' end \n"
		"  if md.origin == nil then return 'origin is nil' end \n"
		"  if md.product == nil then return 'product is nil' end \n"
		"  if md.level == nil then return 'level is nil' end \n"
		"  if md.timerange == nil then return 'timerange is nil' end \n"
		"  if md.area == nil then return 'area is nil' end \n"
		"  if md.proddef == nil then return 'proddef is nil' end \n"
		"  if md.assigneddataset == nil then return 'assigneddataset is nil' end \n"
		"  if md.reftime == nil then return 'reftime is nil' end \n"
		"  if md.bbox ~= nil then return 'bbox is not nil' end \n"
		"  notes = md:notes() \n"
		"  if #notes ~= 1 then return 'table has '..#notes..' elements instead of 1' end \n"
//		"  i = 0 \n"
//		"  for name, value in md.iter do \n"
//		"    i = i + 1 \n"
//		"  end \n"
//		"  if i ~= 8 then return 'iterated '..i..' items instead of 8' end \n"
		"  return nil\n"
		"end \n"
	);
	test.pusharg(md);
	ensure_equals(test.run(), "");
#endif
}

// Serialise using unix file descriptors
template<> template<>
void to::test<8>()
{
	const char* tmpfile = "testmd.tmp";
	fill(md);
    md.source = Source::createBlob("grib", "", "fname", 1, 2);

	// Encode
	int out = open(tmpfile, O_WRONLY | O_CREAT, 0666);
	if (out < 0) throw wibble::exception::File(tmpfile, "opening file");
	md.write(out, tmpfile);
	if (close(out) < 0) throw wibble::exception::File(tmpfile, "closing file");

	// Decode
	ifstream input(tmpfile);
	Metadata md1;
	md1.read(input, tmpfile);
	input.close();

	ensure_equals(md, md1);
}

}

// vim:set ts=4 sw=4:
