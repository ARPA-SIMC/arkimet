#include "metadata.h"
#include <arki/types/tests.h>
#include <arki/tests/lua.h>
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

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
	return o;
}
}

namespace tut {
using namespace std;
using namespace wibble::tests;
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
        md.set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
        md.set(Origin::createGRIB1(1, 2, 3));
        md.set(Product::createGRIB1(1, 2, 3));
        md.set(Level::createGRIB1(114, 12, 34));
        md.set(Timerange::createGRIB1(1, 1, 2, 3));
        md.set(Area::createGRIB(testValues));
        md.set(Proddef::createGRIB(testValues));
        md.set(AssignedDataset::create("dsname", "dsid"));
        md.add_note("test note");
    }

    void ensure_md_matches_prefill(WIBBLE_TEST_LOCPRM, const Metadata& md)
    {
        wassert(actual(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1))) == md.get<Reftime>());
        wassert(actual(Origin::createGRIB1(1, 2, 3)) == md.get<Origin>());
        wassert(actual(Product::createGRIB1(1, 2, 3)) == md.get<Product>());
        wassert(actual(Level::createGRIB1(114, 12, 34)) == md.get<Level>());
        wassert(actual(Timerange::createGRIB1(1, 1, 2, 3)) == md.get<Timerange>());
        wassert(actual(Area::createGRIB(testValues)) == md.get<Area>());
        wassert(actual(Proddef::createGRIB(testValues)) == md.get<Proddef>());
        wassert(actual(AssignedDataset::create("dsname", "dsid")) == md.get<AssignedDataset>());
        wassert(actual(md.notes().size()) == 1);
        wassert(actual((*md.notes().begin()).content) == "test note");
    }
};
TESTGRP(arki_metadata);


// Test sources
template<> template<>
void to::test<1>()
{
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
    wassert(actual(md.source().style()) == Source::BLOB);
    wassert(actual(md.source().format) == "grib");

    const source::Blob& blob = md.sourceBlob();
    wassert(actual(blob.filename) == "fname");
    wassert(actual(blob.offset) == 1u);
    wassert(actual(blob.size) == 2u);
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
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
    fill(md);

    string encoded = md.encodeBinary();
    stringstream stream(encoded, ios_base::in);

	Metadata md1;
	md1.read(stream, "(test memory buffer)");

    wassert(actual(Source::createBlob("grib", "", "fname", 1, 2)) == md1.source());
    wassert(actual(md1.source().format) == "grib");
    wruntest(ensure_md_matches_prefill, md1);


    // Test PERIOD reference times
    md.set(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

    encoded = md.encodeBinary();
    stringstream stream1(encoded, ios_base::in);

	Metadata md2;
	md2.read(stream1, "(test memory buffer)");

    wassert(actual(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3))) == md2.get<Reftime>());
}

// Test Yaml encoding and decoding
template<> template<>
void to::test<3>()
{
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
    fill(md);

	stringstream output;
	md.writeYaml(output);
	stringstream stream(output.str(), ios_base::in);

	Metadata md1;
	md1.readYaml(stream, "(test memory buffer)");

    wassert(actual(Source::createBlob("grib", "", "fname", 1, 2)) == md1.source());
    wassert(actual(md1.source().format) == "grib");
    wruntest(ensure_md_matches_prefill, md1);

    // Test PERIOD reference times
    md.set(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

	stringstream output1;
	md.writeYaml(output1);
	stringstream stream1(output1.str(), ios_base::in);

	Metadata md2;
	md2.readYaml(stream1, "(test memory buffer)");

    wassert(actual(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3))) == md2.get<Reftime>());
}

// Test JSON encoding and decoding
template<> template<>
void to::test<4>()
{
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
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

    wassert(actual(Source::createBlob("grib", "", "fname", 1, 2)) == md1.source());
    wassert(actual(md1.source().format) == "grib");
    wruntest(ensure_md_matches_prefill, md1);


    // Test PERIOD reference times
    md.set(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

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

    wassert(actual(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3))) == md2.get<Reftime>());
}

// Test encoding and decoding with inline data
template<> template<>
void to::test<5>()
{
    // Here is some data
    vector<uint8_t> buf = { 'c', 'i', 'a', 'o' };
    md.set_source_inline("test", vector<uint8_t>(buf));

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
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
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
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));

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
