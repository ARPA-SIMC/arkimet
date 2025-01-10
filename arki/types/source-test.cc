#include "tests.h"
#include "source.h"
#include "source/blob.h"
#include "arki/segment/data.h"
#include "arki/structured/json.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/core/file.h"
#include "arki/core/binary.h"
#include "arki/stream.h"
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
    { "BLOB(grib,testfile:100+50)", "BLOB(bufr,festfile:100+50)", "BLOB(bufr,testfile:99+50)", "BLOB(bufr,testfile:100+49)", },
    "BLOB(bufr,testfile:100+50)",
    { "BLOB(vm2,testfile:100+50)", "BLOB(bufr,zestfile:100+50)", "BLOB(bufr,testfile:101+50)", "BLOB(bufr,testfile:100+51)", "INLINE(bufr,9)", "URL(bufr,foo)", });

add_generic_test("inline",
    { "INLINE(grib, 11)", "INLINE(bufr, 9)", },
    "INLINE(bufr,10)",
    { "INLINE(bufr, 11)", "INLINE(vm2, 9)", });

add_generic_test("url",
    { "URL(grib,zoo)", "URL(bufr,boo)", },
    "URL(bufr,foo)",
    { "URL(bufr,zoo)", "URL(vm2,foo)", });


// Check Blob
add_method("blob_details", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked(DataFormat::JPEG, "", "testfile", 21, 42);
    wassert(actual(o).is_source_blob(DataFormat::JPEG, "", "testfile", 21u, 42u));

	#if 0
	source.prependPath("/pippo");
	source.getBlob(fname, offset, size);
	ensure_equals(fname, "/pippo/testfile");
	ensure_equals(offset, 21u);
	ensure_equals(size, 42u);
	#endif

    wassert(actual(o) == Source::createBlobUnlocked(DataFormat::JPEG, "", "testfile", 21, 42));
    wassert(actual(o) == Source::createBlobUnlocked(DataFormat::JPEG, "/tmp", "testfile", 21, 42));
    wassert(actual(o) != Source::createBlobUnlocked(DataFormat::JPEG, "", "testfile", 22, 43));
    wassert(actual(o) != Source::createURL(DataFormat::JPEG, "testfile"));
    wassert(actual(o) != Source::createInline(DataFormat::JPEG, 43));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Blob on big files
add_method("blob_bigfiles", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked(DataFormat::ODIMH5, "", "testfile", 0x8000FFFFffffFFFFLLU, 42);
    wassert(actual(o).is_source_blob(DataFormat::ODIMH5, "", "testfile", 0x8000FFFFffffFFFFLLU, 42u));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Blob and pathnames handling
add_method("blob_pathnames", [] {
    unique_ptr<source::Blob> o = source::Blob::create_unlocked(DataFormat::VM2, "", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "testfile");

    o = source::Blob::create_unlocked(DataFormat::VM2, "/tmp", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/tmp/testfile");

    o = source::Blob::create_unlocked(DataFormat::VM2, "/tmp", "/antani/testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/antani/testfile");
});

// Check Blob and pathnames handling in serialization
add_method("blob_pathnames_encode", [] {
    unique_ptr<Source> o = Source::createBlobUnlocked(DataFormat::NETCDF, "/tmp", "testfile", 21, 42);

    // Encode to binary, decode, we lose the root
    vector<uint8_t> enc = o->encodeBinary();
    BinaryDecoder dec(enc);
    unique_ptr<Source> decoded = downcast<Source>(types::Type::decode(dec));
    wassert(actual(decoded).is_source_blob(DataFormat::NETCDF, "", "testfile", 21, 42));

    // Encode to YAML, decode, basedir and filename have merged
    stringstream tmp;
    tmp << *o;
    string enc1 = tmp.str();
    decoded = types::Source::decodeString(enc1);
    wassert(actual(decoded).is_source_blob(DataFormat::NETCDF, "", "/tmp/testfile", 21, 42));

    // Encode to JSON, decode, we keep the root
    {
        std::stringstream jbuf;
        structured::JSON json(jbuf);
        o->serialise(json, structured::keys_json);
        structured::Memory parsed;
        structured::JSON::parse(jbuf.str(), parsed);

        decoded = downcast<Source>(types::decode_structure(structured::keys_json, parsed.root()));
        wassert(actual(decoded).is_source_blob(DataFormat::NETCDF, "/tmp", "testfile", 21, 42));
    }
});

add_method("blob_stream", [] {
    auto session = std::make_shared<segment::Session>();
    auto segment = session->segment(DataFormat::GRIB, "inbound", "test.grib1");
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    unique_ptr<source::Blob> o = source::Blob::create(DataFormat::GRIB, "inbound", "test.grib1", 7218, 34960, reader);
    std::filesystem::remove("test.grib");
    {
        auto stream = StreamOutput::create(std::make_shared<File>("test.grib", O_WRONLY | O_CREAT | O_TRUNC));
        wassert(actual(o->stream_data(*stream)) == stream::SendResult());
    }

    std::string data = sys::read_file("test.grib");
    wassert(actual(data.size()) == 34960u);
    wassert(actual(data.substr(0, 4)) == "GRIB");
    wassert(actual(data.substr(data.size() - 4)) == "7777");

    std::filesystem::remove("test.grib");
});

// Check URL
add_method("url_details", [] {
    unique_ptr<Source> o = Source::createURL(DataFormat::GRIB, "http://foobar");
    wassert(actual(o).is_source_url(DataFormat::GRIB, "http://foobar"));

    wassert(actual(o) == Source::createURL(DataFormat::GRIB, "http://foobar"));
    wassert(actual(o) != Source::createURL(DataFormat::GRIB, "http://foobar/a"));
    wassert(actual(o) != Source::createBlobUnlocked(DataFormat::GRIB, "", "http://foobar", 1, 2));
    wassert(actual(o) != Source::createInline(DataFormat::GRIB, 1));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});

// Check Inline
add_method("inline_details", [] {
    unique_ptr<Source> o = Source::createInline(DataFormat::GRIB, 12345);
    wassert(actual(o).is_source_inline(DataFormat::GRIB, 12345u));

    wassert(actual(o) == Source::createInline(DataFormat::GRIB, 12345));
    wassert(actual(o) != Source::createInline(DataFormat::GRIB, 12344));
    wassert(actual(o) != Source::createBlobUnlocked(DataFormat::GRIB, "", "", 0, 12344));
    wassert(actual(o) != Source::createURL(DataFormat::GRIB, ""));

    // Test encoding/decoding
    wassert(actual(o).serializes());
});


}

}
