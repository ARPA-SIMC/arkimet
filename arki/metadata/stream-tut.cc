#include <arki/tests/tests.h>
#include <arki/metadata/stream.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/assigneddataset.h>
#include <arki/utils/sys.h>
#include <iostream>

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
using namespace arki::utils;
using namespace arki::tests;

struct arki_metadata_stream_shar {
	Metadata md;
	ValueBag testValues;

	arki_metadata_stream_shar()
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
		md.set(origin::GRIB1::create(1, 2, 3));
		md.set(product::GRIB1::create(1, 2, 3));
		md.set(level::GRIB1::create(114, 12, 34));
		md.set(timerange::GRIB1::create(1, 1, 2, 3));
		md.set(area::GRIB::create(testValues));
		md.set(proddef::GRIB::create(testValues));
		md.add_note("test note");
		md.set(AssignedDataset::create("dsname", "dsid"));
	}
};
TESTGRP(arki_metadata_stream);


inline bool cmpmd(Metadata& md1, Metadata& md2)
{
    if (md1 != md2)
    {
        cerr << "----- The two metadata differ.  First one:" << endl;
        md1.writeYaml(cerr);
        if (md1.source().style() == Source::INLINE)
        {
            const auto& buf = md1.getData();
            cerr << "-- Inline data:" << string((const char*)buf.data(), buf.size()) << endl;
        }
        cerr << "----- Second one:" << endl;
        md2.writeYaml(cerr);
        if (md2.source().style() == Source::INLINE)
        {
            const auto& buf = md2.getData();
            cerr << "-- Inline data:" << string((const char*)buf.data(), buf.size()) << endl;
        }
        return false;
    }
    return true;
}

// Test metadata stream
template<> template<>
void to::test<1>()
{
    // Create test metadata
    Metadata md1;
    md1.set_source(Source::createBlob("grib", "", "fname", 1, 2));
    this->fill(md1);

	Metadata md2;
	md2 = md1;
	md2.set(origin::BUFR::create(1, 2));

    const char* teststr = "this is a test";
    md1.set_source_inline("test", vector<uint8_t>(teststr, teststr + 14));

    // Encode everything in a buffer
    size_t end1, end2;
    std::string input = tempfile_to_string([&](sys::NamedFileDescriptor& out) {
        md1.write(out, "(memory)");
        end1 = out.lseek(0, SEEK_CUR);
        md2.write(out, "(memory)");
        end2 = out.lseek(0, SEEK_CUR);
    });

	// Where we collect the decoded metadata
	metadata::Collection results;

    // Stream for the decoding
    metadata::Stream mdstream(results.inserter_func(), "test stream");

	size_t cur = 0;

	// Not a full metadata yet
	mdstream.readData(input.data() + cur, end1 - 20);
	cur += end1-20;
	ensure_equals(results.size(), 0u);

	// The full metadata but not the data
	mdstream.readData(input.data() + cur, 10);
	cur += 10;
	ensure_equals(results.size(), 0u);

	// The full metadata and the data and part of the next metadata
	mdstream.readData(input.data() + cur, 40);
	cur += 40;
	ensure_equals(results.size(), 1u);

	// All the rest
	mdstream.readData(input.data() + cur, end2-cur);
	cur = end2;

	// No bytes must be left to decode
	ensure_equals(mdstream.countBytesUnprocessed(), 0u);

	// See that we've got what we expect
	ensure_equals(results.size(), 2u);
	ensure(cmpmd(md1, results[0]));
	ensure(cmpmd(md2, results[1]));
	
	results.clear();

	// Try feeding all the data at the same time
	mdstream.readData(input.data(), input.size());

	// No bytes must be left to decode
	ensure_equals(mdstream.countBytesUnprocessed(), 0u);

	// See that we've got what we expect
	ensure_equals(results.size(), 2u);
	ensure(cmpmd(md1, results[0]));
	ensure(cmpmd(md2, results[1]));
}

// Send data split in less chunks than we have metadata
template<> template<>
void to::test<2>()
{
    // Create test metadata
    Metadata md;
    md.set_source(Source::createBlob("grib", "", "fname", 1, 2));
    this->fill(md);

    // Encode it in a buffer 3 times
    std::string str = tempfile_to_string([&](sys::NamedFileDescriptor& out) {
        md.write(out, "(memory)");
        md.write(out, "(memory)");
        md.write(out, "(memory)");
    });

    // Where we collect the decoded metadata
    metadata::Collection results;

    // Stream for the decoding
    metadata::Stream mdstream(results.inserter_func(), "test stream");

    // Send the data in two halves
    mdstream.readData(str.data(), str.size() / 2);
    ensure_equals(results.size(), 1u);
    mdstream.readData(str.data() + str.size() / 2, str.size() - (str.size() / 2));

    // No bytes must be left to decode
    ensure_equals(mdstream.countBytesUnprocessed(), 0u);

    // See that we've got what we expect
    ensure_equals(results.size(), 3u);
    ensure(cmpmd(md, results[0]));
    ensure(cmpmd(md, results[1]));
    ensure(cmpmd(md, results[2]));
}

}
