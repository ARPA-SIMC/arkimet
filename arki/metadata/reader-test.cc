#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/metadata/tests.h"
#include "arki/types/source/blob.h"
#include "arki/utils/subprocess.h"
#include "arki/utils/sys.h"
#include "reader.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::metadata;
using namespace arki::utils;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_reader");

void Tests::register_tests()
{
    add_method("read_one", [] {
        core::File in("inbound/test.grib1.arkimet", O_RDONLY);
        metadata::BinaryReader reader(in);
        auto md = reader.read();
        wassert(actual(md).contains("reftime", "2007-07-08T13:00:00Z"));
        md = reader.read();
        wassert(actual(md).contains("reftime", "2007-07-07T00:00:00Z"));
        md = reader.read();
        wassert(actual(md).contains("reftime", "2007-10-09T00:00:00Z"));
        wassert_false(reader.read().get());
    });

    add_method("read_all", [] {
        metadata::Collection coll;
        core::File in("inbound/test.grib1.arkimet", O_RDONLY);
        metadata::BinaryReader reader(in);
        wassert_true(reader.read_all(coll.inserter_func()));

        wassert(actual(coll.size()) == 3u);
        wassert(actual(coll[0]).contains("reftime", "2007-07-08T13:00:00Z"));
        wassert(actual(coll[1]).contains("reftime", "2007-07-07T00:00:00Z"));
        wassert(actual(coll[2]).contains("reftime", "2007-10-09T00:00:00Z"));
    });

    add_method("wrongsize", [] {
        core::File fd("test.md", O_WRONLY | O_CREAT | O_TRUNC);
        fd.write("MD\0\0\x0f\xff\xff\xfftest", 12);
        fd.close();

        unsigned count = 0;
        core::File in("test.md", O_RDONLY);
        metadata::BinaryReader reader(in);
        reader.read_all([&](std::shared_ptr<Metadata>) noexcept {
            ++count;
            return true;
        });
        wassert(actual(count) == 0u);
    });

    add_method("issue107_binary", [] {
        unsigned count = 0;
        core::File in("inbound/issue107.yaml", O_RDONLY);
        metadata::BinaryReader md_reader(in);
        try
        {
            md_reader.read_all([&](std::shared_ptr<Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(0) == 1);
        }
        catch (std::runtime_error& e)
        {
            wassert(actual(e.what()).contains(
                "inbound/issue107.yaml: unsupported metadata signature 'So'"));
        }
        wassert(actual(count) == 0u);
    });

    add_method("read_partial", [] {
        // Create a mock input file with inline data
        sys::Tempfile tf;
        metadata::DataManager& data_manager = metadata::DataManager::get();
        core::File in0("inbound/test.grib1.arkimet", O_RDONLY);
        metadata::BinaryReader reader0(in0);
        reader0.read_all([&](std::shared_ptr<Metadata> md) {
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
        metadata::BinaryReader reader(in);

        unsigned count = 0;
        wassert(reader.read_all([&](std::shared_ptr<Metadata>) noexcept {
            ++count;
            return true;
        }));
        wassert(actual(count) == 3u);

        wassert(actual(child.wait()) == 0);
    });

    // Test encoding and decoding with inline data
    add_method("binary_inline", [] {
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
        metadata::BinaryReader reader(temp1);
        auto md1 = reader.read();

        wassert(actual(md1->get_data().read()) == buf);
    });

    // TODO: check reader offset
    // TODO: test iterating bundles without decoding
    // TODO: test reading or not reading inline data
}

} // namespace
