#include "metadata.h"
#include "metadata/collection.h"
#include "core/file.h"
#include "metadata/tests.h"
#include "tests/lua.h"
#include "types/origin.h"
#include "types/product.h"
#include "types/level.h"
#include "types/timerange.h"
#include "types/reftime.h"
#include "types/area.h"
#include "types/proddef.h"
#include "types/assigneddataset.h"
#include "types/source/blob.h"
#include "binary.h"
#include "emitter/json.h"
#include "emitter/memory.h"
#include "utils/sys.h"
#include "utils/files.h"
#include <fcntl.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

struct Fixture : public arki::utils::tests::Fixture
{
    ValueBag testValues;

    Fixture()
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

    void ensure_md_matches_prefill(const Metadata& md)
    {
        wassert(actual(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1))) == md.get<Reftime>());
        wassert(actual(Origin::createGRIB1(1, 2, 3)) == md.get<Origin>());
        wassert(actual(Product::createGRIB1(1, 2, 3)) == md.get<Product>());
        wassert(actual(Level::createGRIB1(114, 12, 34)) == md.get<Level>());
        wassert(actual(Timerange::createGRIB1(1, 1, 2, 3)) == md.get<Timerange>());
        wassert(actual(Area::createGRIB(testValues)) == md.get<Area>());
        wassert(actual(Proddef::createGRIB(testValues)) == md.get<Proddef>());
        wassert(actual(AssignedDataset::create("dsname", "dsid")) == md.get<AssignedDataset>());
        wassert(actual(md.notes().size()) == 1u);
        wassert(actual((*md.notes().begin()).content) == "test note");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_metadata");


void Tests::register_tests() {

// Test sources
add_method("sources", [](Fixture& f) {
    Metadata md;
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));
    wassert(actual(md.source().style()) == Source::BLOB);
    wassert(actual(md.source().format) == "grib");

    const source::Blob& blob = md.sourceBlob();
    wassert(actual(blob.filename) == "inbound/test.grib1");
    wassert(actual(blob.offset) == 1u);
    wassert(actual(blob.size) == 2u);
});

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

// Test binary encoding and decoding
add_method("binary", [](Fixture& f) {
    std::string dir = sys::abspath(".");

    Metadata md;
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));
    f.fill(md);

    vector<uint8_t> encoded = md.encodeBinary();
    wassert(actual((char)encoded[0]) == 'M');
    wassert(actual((char)encoded[1]) == 'D');
    sys::write_file("test.md", encoded.data(), encoded.size());

    Metadata md1;
    BinaryDecoder dec(encoded);
    wassert(md1.read(dec, metadata::ReadContext("(test memory buffer)", dir)));

    wassert(actual_type(md1.source()).is_source_blob("grib", dir, "inbound/test.grib1", 1, 2));
    wassert(actual(md1.source().format) == "grib");
    wassert(f.ensure_md_matches_prefill(md1));


    // Test PERIOD reference times
    md.set(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

    encoded = md.encodeBinary();
    Metadata md2;
    BinaryDecoder dec1(encoded);
    wassert(md2.read(dec1, metadata::ReadContext("(test memory buffer)", ""), false));

    wassert(actual(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3))) == md2.get<Reftime>());


    // Test methods to load metadata from files
    metadata::Collection mds;

    wassert(Metadata::read_file(metadata::ReadContext("test.md", dir), mds.inserter_func()));
    wassert(actual(mds.size()) == 1u);
    wassert(actual_type(mds[0].source()).is_source_blob("grib", dir, "inbound/test.grib1", 1, 2));
    mds.clear();

    wassert(Metadata::read_file("test.md", mds.inserter_func()));
    wassert(actual(mds.size()) == 1u);
    wassert(actual_type(mds[0].source()).is_source_blob("grib", dir, "inbound/test.grib1", 1, 2));

    /*
    /// Read all metadata from a file into the given consumer
    static void read_file(const metadata::ReadContext& fname, metadata_dest_func dest);
    */
});

// Test Yaml encoding and decoding
add_method("yaml", [](Fixture& f) {
    Metadata md;
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));
    f.fill(md);

    stringstream output;
    md.writeYaml(output);
    Metadata md1;
    string s(output.str());
    auto reader = LineReader::from_chars(s.data(), s.size());
    md1.readYaml(*reader, "(test memory buffer)");

    wassert(actual(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2)) == md1.source());
    wassert(actual(md1.source().format) == "grib");
    wassert(f.ensure_md_matches_prefill(md1));

    // Test PERIOD reference times
    md.set(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));

    stringstream output1;
    md.writeYaml(output1);
    Metadata md2;
    s = output1.str();
    reader = LineReader::from_chars(s.data(), s.size());
    md2.readYaml(*reader, "(test memory buffer)");

    wassert(actual(Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3))) == md2.get<Reftime>());
});

// Test JSON encoding and decoding
add_method("json", [](Fixture& f) {
    Metadata md;
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));
    f.fill(md);

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

    wassert(actual(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2)) == md1.source());
    wassert(actual(md1.source().format) == "grib");
    wassert(f.ensure_md_matches_prefill(md1));


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
});

// Test encoding and decoding with inline data
add_method("binary_inline", [](Fixture& f) {
    Metadata md;
    // Here is some data
    vector<uint8_t> buf = { 'c', 'i', 'a', 'o' };
    md.set_source_inline("test", vector<uint8_t>(buf));

    // Encode
    sys::File temp("testfile", O_WRONLY | O_CREAT | O_TRUNC, 0666);
    wassert(md.write(temp));
    temp.close();

    // Decode
    Metadata md1;
    sys::File temp1("testfile", O_RDONLY);
    wassert(md1.read(temp1, metadata::ReadContext("testfile"), true));
    temp1.close();

    ensure(md1.getData() == buf);
});

// Serialise using unix file descriptors
add_method("binary_fd", [](Fixture& f) {
    Metadata md;
    const char* tmpfile = "testmd.tmp";
    f.fill(md);
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));

    // Encode
    sys::File out(tmpfile, O_WRONLY | O_CREAT, 0666);
    md.write(out);
    out.close();

    // Decode
    sys::File in(tmpfile, O_RDONLY);
    Metadata md1;
    md1.read(in, metadata::ReadContext(tmpfile));
    in.close();

    wassert(actual(md) == md1);
});

// Reproduce decoding error at #24
add_method("decode_issue_24", [](Fixture& f) {
    unsigned count = 0;
    Metadata::read_file("inbound/issue24.arkimet", [&](unique_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 1u);
});

// Test Lua functions
add_method("lua", [](Fixture& f) {
    Metadata md;
#ifdef HAVE_LUA
    md.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 1, 2));
    f.fill(md);

    arki::tests::Lua test(
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
});

add_method("stream", [](Fixture& f) {
    metadata::Collection grib("inbound/test.grib1");
    metadata::Collection bufr("inbound/test.bufr");
    metadata::Collection vm2("inbound/test.vm2");
    metadata::Collection odim("inbound/odimh5/XSEC_v21.h5");

    File fd("tmpfile", O_WRONLY | O_CREAT | O_TRUNC);
    wassert(actual(grib[0].stream_data(fd)) == grib[0].sourceBlob().size);
    fd.close();
    wassert(actual(sys::size("tmpfile")) == grib[0].sourceBlob().size);

    fd.open(O_WRONLY | O_CREAT | O_TRUNC);
    wassert(actual(bufr[0].stream_data(fd)) == bufr[0].sourceBlob().size);
    fd.close();
    wassert(actual(sys::size("tmpfile")) == bufr[0].sourceBlob().size);

    fd.open(O_WRONLY | O_CREAT | O_TRUNC);
    wassert(actual(vm2[0].stream_data(fd)) == vm2[0].sourceBlob().size + 1);
    fd.close();
    wassert(actual(sys::size("tmpfile")) == vm2[0].sourceBlob().size + 1);

    fd.open(O_WRONLY | O_CREAT | O_TRUNC);
    wassert(actual(odim[0].stream_data(fd)) == odim[0].sourceBlob().size);
    fd.close();
    wassert(actual(sys::size("tmpfile")) == odim[0].sourceBlob().size);
});

add_method("issue107_binary", [](Fixture& f) {
    unsigned count = 0;
    try {
        Metadata::read_file("inbound/issue107.yaml", [&](unique_ptr<Metadata> md) { ++count; return true; });
        wassert(actual(0) == 1);
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()).contains("metadata entry does not start with "));
    }
    wassert(actual(count) == 0u);
});

add_method("issue107_yaml", [](Fixture& f) {
    Metadata md;
    File fd("inbound/issue107.yaml", O_RDONLY);
    auto reader = LineReader::from_fd(fd);

    wassert(actual(md.readYaml(*reader, "inbound/issue107.yaml")).istrue());
});

add_method("wrongsize", [](Fixture& f) {
    File fd("test.md", O_WRONLY | O_CREAT | O_TRUNC);
    fd.write("MD\0\0\x0f\xff\xff\xfftest", 12);
    fd.close();

    unsigned count = 0;
    Metadata::read_file("test.md", [&](unique_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 0u);
});

}

}
