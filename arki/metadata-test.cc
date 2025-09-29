#include "core/binary.h"
#include "core/file.h"
#include "metadata.h"
#include "metadata/collection.h"
#include "metadata/data.h"
#include "metadata/reader.h"
#include "metadata/tests.h"
#include "stream.h"
#include "structured/json.h"
#include "structured/keys.h"
#include "structured/memory.h"
#include "types/area.h"
#include "types/assigneddataset.h"
#include "types/level.h"
#include "types/note.h"
#include "types/origin.h"
#include "types/proddef.h"
#include "types/product.h"
#include "types/reftime.h"
#include "types/source/blob.h"
#include "types/timerange.h"
#include "types/values.h"
#include "utils/files.h"
#include "utils/subprocess.h"
#include "utils/sys.h"
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
        using namespace arki::types::values;
        // 100663295 == 0x5ffffff
        testValues = ValueBag::parse(
            "foo=5, bar=5000, baz=-200, moo=100663295, antani=-1, blinda=0, "
            "supercazzola=-1234567, pippo=pippo, pluto=\"12\", cippo=\"\"");
    }

    void fill(Metadata& md)
    {
        md.test_set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
        md.test_set(Origin::createGRIB1(1, 2, 3));
        md.test_set(Product::createGRIB1(1, 2, 3));
        md.test_set(Level::createGRIB1(114, 12, 34));
        md.test_set(Timerange::createGRIB1(1, 1, 2, 3));
        md.test_set<area::GRIB>(testValues);
        md.test_set(Proddef::createGRIB(testValues));
        md.add_note("test note");
    }

    void ensure_md_matches_prefill(const Metadata& md)
    {
        wassert(actual(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1))) ==
                md.get<Reftime>());
        wassert(actual(Origin::createGRIB1(1, 2, 3)) == md.get<Origin>());
        wassert(actual(Product::createGRIB1(1, 2, 3)) == md.get<Product>());
        wassert(actual(Level::createGRIB1(114, 12, 34)) == md.get<Level>());
        wassert(actual(Timerange::createGRIB1(1, 1, 2, 3)) ==
                md.get<Timerange>());
        wassert(actual(area::GRIB::create(testValues)) == md.get<Area>());
        wassert(actual(Proddef::createGRIB(testValues)) == md.get<Proddef>());
        auto notes = md.notes();
        wassert(actual(notes.second - notes.first) == 1u);
        core::Time time;
        std::string content;
        reinterpret_cast<const types::Note*>(*notes.first)->get(time, content);
        wassert(actual(content) == "test note");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_metadata");

template <typename TESTDATA> void test_inline()
{
    TESTDATA td;

    // Write it out as inline metadata
    File wfd("test.md", O_WRONLY | O_CREAT | O_TRUNC);
    for (auto& md : td.mds)
    {
        md->makeInline();
        md->write(wfd);
    }
    wfd.close();

    unsigned count = 0;
    core::File in("test.md", O_RDONLY);
    metadata::BinaryReader md_reader(in);
    md_reader.read_all([&](std::shared_ptr<Metadata> md) {
        md->get_data();
        ++count;
        return true;
    });

    wassert(actual(count) == 3u);
}

void Tests::register_tests()
{

    // Test sources
    add_method("sources", [](Fixture& f) {
        Metadata md;
        md.set_source(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                 "inbound/test.grib1", 1, 2));
        wassert(actual(md.source().style()) == Source::Style::BLOB);
        wassert(actual(md.source().format) == DataFormat::GRIB);

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
        std::filesystem::path dir = std::filesystem::current_path();

        Metadata md;
        md.set_source(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                 "inbound/test.grib1", 1, 2));
        f.fill(md);

        vector<uint8_t> encoded = md.encodeBinary();
        wassert(actual((char)encoded[0]) == 'M');
        wassert(actual((char)encoded[1]) == 'D');
        sys::write_file("test.md", encoded.data(), encoded.size());

        core::File in("test.md", O_RDONLY);
        metadata::BinaryReader reader(in);
        auto md1 = reader.read();
        wassert_true(md1.get());

        wassert(actual_type(md1->source())
                    .is_source_blob(DataFormat::GRIB, dir, "inbound/test.grib1",
                                    1, 2));
        wassert(actual(md1->source().format) == DataFormat::GRIB);
        wassert(f.ensure_md_matches_prefill(*md1));
    });

    // Test JSON encoding and decoding
    add_method("json", [](Fixture& f) {
        Metadata md;
        md.set_source(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                 "inbound/test.grib1", 1, 2));
        f.fill(md);

        // Serialise to JSON;
        stringstream output;
        structured::JSON json(output);
        md.serialise(json, structured::keys_json);

        // Parse back
        structured::Memory parsed;
        structured::JSON::parse(output.str(), parsed);

        auto md1 =
            Metadata::read_structure(structured::keys_json, parsed.root());

        wassert(actual(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                  "inbound/test.grib1", 1,
                                                  2)) == md1->source());
        wassert(actual(md1->source().format) == DataFormat::GRIB);
        wassert(f.ensure_md_matches_prefill(*md1));
    });

    // Serialise using unix file descriptors
    add_method("binary_fd", [](Fixture& f) {
        Metadata md;
        const char* tmpfile = "testmd.tmp";
        f.fill(md);
        md.set_source(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                 "inbound/test.grib1", 1, 2));

        // Encode
        sys::File out(tmpfile, O_WRONLY | O_CREAT, 0666);
        md.write(out);
        out.close();

        // Decode
        sys::File in(tmpfile, O_RDONLY);
        metadata::BinaryReader reader(in);
        auto md1 = wcallchecked(reader.read());

        wassert(actual(md) == *md1);
    });

    // Reproduce decoding error at #24
    add_method("decode_issue_24", [](Fixture& f) {
        unsigned count = 0;
        core::File in("inbound/issue24.arkimet", O_RDONLY);
        metadata::BinaryReader md_reader(in);
        md_reader.read_all([&](std::shared_ptr<Metadata> md) noexcept {
            ++count;
            return true;
        });
        wassert(actual(count) == 1u);
    });

    add_method("stream_grib", [](Fixture& f) {
        skip_unless_grib();
        metadata::TestCollection grib("inbound/fixture.grib1");
        wassert(actual(grib.size()) == 3u);
        {
            auto stream = StreamOutput::create(std::make_shared<File>(
                "tmpfile", O_WRONLY | O_CREAT | O_TRUNC));
            wassert(actual(grib[0].stream_data(*stream)) ==
                    stream::SendResult());
        }
        wassert(actual(sys::size("tmpfile")) == grib[0].sourceBlob().size);
    });

    add_method("stream_bufr", [](Fixture& f) {
        skip_unless_bufr();
        metadata::TestCollection bufr("inbound/test.bufr");
        {
            auto stream = StreamOutput::create(std::make_shared<File>(
                "tmpfile", O_WRONLY | O_CREAT | O_TRUNC));
            wassert(actual(bufr[0].stream_data(*stream)) ==
                    stream::SendResult());
        }
        wassert(actual(sys::size("tmpfile")) == bufr[0].sourceBlob().size);
    });

    add_method("stream_vm2", [](Fixture& f) {
        skip_unless_vm2();
        metadata::TestCollection vm2("inbound/test.vm2");
        {
            auto stream = StreamOutput::create(std::make_shared<File>(
                "tmpfile", O_WRONLY | O_CREAT | O_TRUNC));
            wassert(actual(vm2[0].stream_data(*stream)) ==
                    stream::SendResult());
        }
        wassert(actual(sys::size("tmpfile")) == vm2[0].sourceBlob().size + 1);
    });

    add_method("stream_odim", [](Fixture& f) {
        metadata::TestCollection odim("inbound/odimh5/XSEC_v21.h5");
        {
            auto stream = StreamOutput::create(std::make_shared<File>(
                "tmpfile", O_WRONLY | O_CREAT | O_TRUNC));
            wassert(actual(odim[0].stream_data(*stream)) ==
                    stream::SendResult());
        }
        wassert(actual(sys::size("tmpfile")) == odim[0].sourceBlob().size);
    });

    add_method("inline_grib",
               [](Fixture& f) { wassert(test_inline<GRIBData>()); });
    add_method("inline_bufr",
               [](Fixture& f) { wassert(test_inline<BUFRData>()); });
    add_method("inline_vm2",
               [](Fixture& f) { wassert(test_inline<VM2Data>()); });
    add_method("inline_odimh5",
               [](Fixture& f) { wassert(test_inline<ODIMData>()); });

    add_method("issue256", [](Fixture& f) {
        std::shared_ptr<Metadata> md;
        core::File in("inbound/issue256.arkimet", O_RDONLY);
        metadata::BinaryReader reader(in);
        reader.read_all([&](std::shared_ptr<Metadata> m) {
            wassert_false(md.get());
            md = m;
            return true;
        });

        wassert_true(md.get());

        wassert(actual(md->get(TYPE_TIMERANGE)->to_string()) ==
                "GRIB1(000, 228h)");

        // Source: URL(grib,http://arkiope.metarpa:8090/dataset/ifs_ita010)
        // Origin: GRIB1(098, 000, 151)
        // Product: GRIB1(098, 128, 228)
        // Level: GRIB1(001)
        // Timerange: GRIB1(000, 228h)
        // Reftime: 2021-01-10T12:00:00Z
        // Area: GRIB(Ni=181, Nj=161, latfirst=50000000, latlast=34000000,
        // lonfirst=2000000, lonlast=20000000, type=0) Proddef: GRIB(ld=1, mt=9,
        // nn=0, tod=1) Run: MINUTE(12:00) Note: [2021-01-10T19:05:15Z]Scanned
        // from JND01101200012000001.grib:3051848+58390
    });
}

} // namespace
