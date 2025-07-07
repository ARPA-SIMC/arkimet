#include "core/binary.h"
#include "core/file.h"
#include "metadata.h"
#include "metadata/collection.h"
#include "metadata/data.h"
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
    Metadata::read_file("test.md", [&](std::shared_ptr<Metadata> md) {
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

        std::shared_ptr<Metadata> md1;
        core::BinaryDecoder dec(encoded);
        md1 = wcallchecked(Metadata::read_binary(
            dec, metadata::ReadContext("(test memory buffer)", dir)));
        wassert_true(md1);

        wassert(actual_type(md1->source())
                    .is_source_blob(DataFormat::GRIB, dir, "inbound/test.grib1",
                                    1, 2));
        wassert(actual(md1->source().format) == DataFormat::GRIB);
        wassert(f.ensure_md_matches_prefill(*md1));

        // Test methods to load metadata from files
        metadata::Collection mds;

        wassert(Metadata::read_file(metadata::ReadContext("test.md", dir),
                                    mds.inserter_func()));
        wassert(actual(mds.size()) == 1u);
        wassert(actual_type(mds[0].source())
                    .is_source_blob(DataFormat::GRIB, dir, "inbound/test.grib1",
                                    1, 2));
        mds.clear();

        wassert(Metadata::read_file("test.md", mds.inserter_func()));
        wassert(actual(mds.size()) == 1u);
        wassert(actual_type(mds[0].source())
                    .is_source_blob(DataFormat::GRIB, dir, "inbound/test.grib1",
                                    1, 2));

        /*
        /// Read all metadata from a file into the given consumer
        static void read_file(const metadata::ReadContext& fname,
        metadata_dest_func dest);
        */
    });

    // Test Yaml encoding and decoding
    add_method("yaml", [](Fixture& f) {
        Metadata md;
        md.set_source(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                 "inbound/test.grib1", 1, 2));
        f.fill(md);

        string s    = md.to_yaml();
        auto reader = LineReader::from_chars(s.data(), s.size());
        auto md1    = Metadata::read_yaml(*reader, "(test memory buffer)");
        wassert_true(md1);
        wassert(actual(Source::createBlobUnlocked(DataFormat::GRIB, "",
                                                  "inbound/test.grib1", 1,
                                                  2)) == md1->source());
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

    // Test encoding and decoding with inline data
    add_method("binary_inline", [](Fixture& f) {
        Metadata md;
        // Here is some data
        vector<uint8_t> buf = {'c', 'i', 'a', 'o'};
        md.set_source_inline(DataFormat::GRIB,
                             metadata::DataManager::get().to_data(
                                 DataFormat::GRIB, vector<uint8_t>(buf)));

        // Encode
        sys::File temp("testfile", O_WRONLY | O_CREAT | O_TRUNC, 0666);
        wassert(md.write(temp));
        temp.close();

        // Decode
        sys::File temp1("testfile", O_RDONLY);
        auto md1 = wcallchecked(Metadata::read_binary(
            temp1, metadata::ReadContext("testfile"), true));
        temp1.close();

        wassert(actual(md1->get_data().read()) == buf);
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
        auto md1 = wcallchecked(
            Metadata::read_binary(in, metadata::ReadContext(tmpfile)));
        in.close();

        wassert(actual(md) == *md1);
    });

    // Reproduce decoding error at #24
    add_method("decode_issue_24", [](Fixture& f) {
        unsigned count = 0;
        Metadata::read_file("inbound/issue24.arkimet",
                            [&](std::shared_ptr<Metadata> md) noexcept {
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

    add_method("issue107_binary", [](Fixture& f) {
        unsigned count = 0;
        try
        {
            Metadata::read_file("inbound/issue107.yaml",
                                [&](std::shared_ptr<Metadata> md) noexcept {
                                    ++count;
                                    return true;
                                });
            wassert(actual(0) == 1);
        }
        catch (std::runtime_error& e)
        {
            wassert(actual(e.what()).contains(
                "metadata entry does not start with "));
        }
        wassert(actual(count) == 0u);
    });

    add_method("issue107_yaml", [](Fixture& f) {
        Metadata md;
        File fd("inbound/issue107.yaml", O_RDONLY);
        auto reader = LineReader::from_fd(fd);

        wassert(actual(Metadata::read_yaml(*reader, "inbound/issue107.yaml"))
                    .istrue());
    });

    add_method("wrongsize", [](Fixture& f) {
        File fd("test.md", O_WRONLY | O_CREAT | O_TRUNC);
        fd.write("MD\0\0\x0f\xff\xff\xfftest", 12);
        fd.close();

        unsigned count = 0;
        Metadata::read_file("test.md",
                            [&](std::shared_ptr<Metadata> md) noexcept {
                                ++count;
                                return true;
                            });
        wassert(actual(count) == 0u);
    });

    add_method("read_partial", [](Fixture& f) {
        // Create a mock input file with inline data
        sys::Tempfile tf;
        metadata::DataManager& data_manager = metadata::DataManager::get();
        Metadata::read_file(
            "inbound/test.grib1.arkimet", [&](std::shared_ptr<Metadata> md) {
                md->set_source_inline(
                    DataFormat::BUFR,
                    data_manager.to_data(
                        DataFormat::BUFR,
                        std::vector<uint8_t>(md->sourceBlob().size)));
                md->write(tf);
                return true;
            });
        // fprintf(stderr, "TO READ: %d\n", (int)tf.lseek(0, SEEK_CUR));
        tf.lseek(0);

        // Read it a bit at a time, to try to cause partial reads in the reader
        struct Writer : public subprocess::Child
        {
            sys::NamedFileDescriptor& in;

            Writer(sys::NamedFileDescriptor& in) : in(in) {}

            int main() noexcept override
            {
                sys::NamedFileDescriptor out(1, "stdout");
                while (true)
                {
                    char buf[1];
                    if (!in.read(buf, 1))
                        break;

                    out.write_all_or_throw(buf, 1);
                }
                return 0;
            }
        };

        Writer child(tf);
        child.pass_fds.push_back(tf);
        child.set_stdout(subprocess::Redirect::PIPE);
        child.fork();

        sys::NamedFileDescriptor in(child.get_stdout(), "child pipe");

        unsigned count = 0;
        wassert(
            Metadata::read_file(in, [&](std::shared_ptr<Metadata>) noexcept {
                ++count;
                return true;
            }));
        wassert(actual(count) == 3u);

        wassert(actual(child.wait()) == 0);
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
        Metadata::read_file("inbound/issue256.arkimet",
                            [&](std::shared_ptr<Metadata> m) {
                                wassert_false(md);
                                md = m;
                                return true;
                            });

        wassert_true(md);

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
