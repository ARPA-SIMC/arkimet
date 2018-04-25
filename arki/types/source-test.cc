#include "tests.h"
#include "source.h"
#include "source/blob.h"
#include "arki/segment.h"
#include "arki/emitter/json.h"
#include "arki/emitter/memory.h"
#include "arki/core/file.h"
#include "arki/binary.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::types;

class Tests : public TypeTestCase<types::Source>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_source");

void Tests::register_tests() {

add_generic_test("blob",
    { "BLOB(aaa,testfile:100+50)", "BLOB(bufr,festfile:100+50)", "BLOB(bufr,testfile:99+50)", "BLOB(bufr,testfile:100+49)", },
    "BLOB(bufr,testfile:100+50)",
    { "BLOB(grib,testfile:100+50)", "BLOB(bufr,zestfile:100+50)", "BLOB(bufr,testfile:101+50)", "BLOB(bufr,testfile:100+51)", "INLINE(bufr,9)", "URL(bufr,foo)", });

add_generic_test("inline",
    { "INLINE(aaa, 11)", "INLINE(bufr, 9)", },
    "INLINE(bufr,10)",
    { "INLINE(bufr, 11)", "INLINE(ccc, 9)", });

add_generic_test("url",
    { "URL(aaa,zoo)", "URL(bufr,boo)", },
    "URL(bufr,foo)",
    { "URL(bufr,zoo)", "URL(ccc,foo)", });


// Check Blob
add_method("blob_details", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked("test", "", "testfile", 21, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 21u, 42u));

	#if 0
	source.prependPath("/pippo");
	source.getBlob(fname, offset, size);
	ensure_equals(fname, "/pippo/testfile");
	ensure_equals(offset, 21u);
	ensure_equals(size, 42u);
	#endif

    wassert(actual(o) == Source::createBlobUnlocked("test", "", "testfile", 21, 42));
    wassert(actual(o) == Source::createBlobUnlocked("test", "/tmp", "testfile", 21, 42));
    wassert(actual(o) != Source::createBlobUnlocked("test", "", "testfile", 22, 43));
    wassert(actual(o) != Source::createURL("test", "testfile"));
    wassert(actual(o) != Source::createInline("test", 43));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Blob on big files
add_method("blob_bigfiles", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42u));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Blob and pathnames handling
add_method("blob_pathnames", [] {
    unique_ptr<source::Blob> o = source::Blob::create_unlocked("test", "", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "testfile");

    o = source::Blob::create_unlocked("test", "/tmp", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/tmp/testfile");

    o = source::Blob::create_unlocked("test", "/tmp", "/antani/testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/antani/testfile");
});

// Check Blob and pathnames handling in serialization
add_method("blob_pathnames_encode", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked("test", "/tmp", "testfile", 21, 42);

    // Encode to binary, decode, we lose the root
    vector<uint8_t> enc = o->encodeBinary();
    BinaryDecoder dec(enc);
    unique_ptr<Source> decoded = downcast<Source>(types::decode(dec));
    wassert(actual(decoded).is_source_blob("test", "", "testfile", 21, 42));

    // Encode to YAML, decode, basedir and filename have merged
    stringstream tmp;
    tmp << *o;
    string enc1 = tmp.str();
    decoded = types::Source::decodeString(enc1);
    wassert(actual(decoded).is_source_blob("test", "", "/tmp/testfile", 21, 42));

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
});

add_method("blob_stream", [] {
    auto reader = Segment::make_reader("grib", "inbound", "test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());
    unique_ptr<source::Blob> o = source::Blob::create("test", "inbound", "test.grib1", 7218, 34960, reader);
    sys::unlink_ifexists("test.grib");
    File out("test.grib", O_WRONLY | O_CREAT | O_TRUNC);
    wassert(actual(o->stream_data(out)) == 34960u);
    out.close();

    std::string data = sys::read_file("test.grib");
    wassert(actual(data.size()) == 34960u);
    wassert(actual(data.substr(0, 4)) == "GRIB");
    wassert(actual(data.substr(data.size() - 4)) == "7777");

    sys::unlink_ifexists("test.grib");
});

// Check URL
add_method("url_details", [] {
    unique_ptr<Source> o = Source::createURL("test", "http://foobar");
    wassert(actual(o).is_source_url("test", "http://foobar"));

    wassert(actual(o) == Source::createURL("test", "http://foobar"));
    wassert(actual(o) != Source::createURL("test", "http://foobar/a"));
    wassert(actual(o) != Source::createBlobUnlocked("test", "", "http://foobar", 1, 2));
    wassert(actual(o) != Source::createInline("test", 1));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Inline
add_method("inline_details", [] {
    unique_ptr<Source> o = Source::createInline("test", 12345);
    wassert(actual(o).is_source_inline("test", 12345u));

    wassert(actual(o) == Source::createInline("test", 12345));
    wassert(actual(o) != Source::createInline("test", 12344));
    wassert(actual(o) != Source::createBlobUnlocked("test", "", "", 0, 12344));
    wassert(actual(o) != Source::createURL("test", ""));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});


}

}
