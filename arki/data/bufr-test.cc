#include "arki/data/bufr.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/metadata/tests.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_data_bufr");

class ConcreteScanner : public arki::data::bufr::Scanner
{
protected:
    void scan_extra(dballe::BinaryMessage& rmsg,
                    std::shared_ptr<dballe::Message> msg,
                    std::shared_ptr<Metadata> md) override
    {
    }
};

void Tests::register_tests()
{
    add_method("scan_file", [] {
        auto basedir = std::filesystem::current_path() / "inbound";
        auto src     = basedir / "fixture.bufr";
        ConcreteScanner scanner;
        std::vector<std::shared_ptr<Metadata>> mds;
        wassert_true(scanner.scan_file(src, [&](std::shared_ptr<Metadata> md_) {
            mds.emplace_back(md_);
            return true;
        }));
        wassert(actual(mds.size()) == 3u);

        wassert(actual_type(mds[0]->source())
                    .is_source_blob(DataFormat::BUFR, basedir, "fixture.bufr",
                                    0, 146));
        wassert_true(mds[0]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[0]->sourceBlob().read_data());

        wassert(actual_type(mds[1]->source())
                    .is_source_blob(DataFormat::BUFR, basedir, "fixture.bufr",
                                    146, 172));
        wassert_true(mds[1]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[1]->sourceBlob().read_data());

        wassert(actual_type(mds[2]->source())
                    .is_source_blob(DataFormat::BUFR, basedir, "fixture.bufr",
                                    318, 172));
        wassert_true(mds[2]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[2]->sourceBlob().read_data());
    });

    add_method("scan_pipe", [] {
        auto basedir = std::filesystem::current_path() / "inbound";
        auto src     = basedir / "fixture.bufr";
        ConcreteScanner scanner;
        std::vector<std::shared_ptr<Metadata>> mds;
        core::File in(src, O_RDONLY);
        wassert_true(scanner.scan_pipe(in, [&](std::shared_ptr<Metadata> md) {
            mds.emplace_back(md);
            return true;
        }));
        wassert(actual(mds.size()) == 3u);

        wassert(actual_type(mds[0]->source())
                    .is_source_inline(DataFormat::BUFR, 146));
        wassert_true(mds[0]->has_cached_data());

        wassert(actual_type(mds[1]->source())
                    .is_source_inline(DataFormat::BUFR, 172));
        wassert_true(mds[1]->has_cached_data());

        wassert(actual_type(mds[2]->source())
                    .is_source_inline(DataFormat::BUFR, 172));
        wassert_true(mds[2]->has_cached_data());
    });

    add_method("scan_data", [] {
        auto basedir         = std::filesystem::current_path() / "inbound";
        auto src             = basedir / "acars.bufr";
        std::string data_str = sys::read_file(src);
        std::vector<uint8_t> data(data_str.begin(), data_str.end());

        ConcreteScanner scanner;
        std::shared_ptr<Metadata> md = scanner.scan_data(data);
        wassert_true(md.get());
        wassert(actual_type(md->source())
                    .is_source_inline(DataFormat::BUFR, data_str.size()));
        wassert_true(md->has_cached_data());
    });

    // Test validation
    add_method("validate", [] {
        Metadata md;
        vector<uint8_t> buf;

        const data::Validator& v = data::bufr::validator();

        sys::File fd("inbound/test.bufr", O_RDONLY);
        v.validate_file(fd, 0, 194);
        v.validate_file(fd, 194, 220);
        v.validate_file(fd, 414, 220);

        wassert_throws(std::runtime_error, v.validate_file(fd, 1, 193));
        wassert_throws(std::runtime_error, v.validate_file(fd, 0, 193));
        wassert_throws(std::runtime_error, v.validate_file(fd, 0, 195));
        wassert_throws(std::runtime_error, v.validate_file(fd, 193, 221));
        wassert_throws(std::runtime_error, v.validate_file(fd, 414, 221));
        wassert_throws(std::runtime_error, v.validate_file(fd, 634, 0));
        wassert_throws(std::runtime_error, v.validate_file(fd, 634, 10));
        fd.close();

        metadata::TestCollection mdc("inbound/test.bufr");
        buf = mdc[0].get_data().read();

        wassert(v.validate_data(mdc[0].get_data()));
        wassert_throws(
            std::runtime_error,
            v.validate_buf((const char*)buf.data() + 1, buf.size() - 1));
        wassert_throws(std::runtime_error,
                       v.validate_buf(buf.data(), buf.size() - 1));
    });

    // Test reading update sequence numbers
    add_method("usn", [] {
        auto scanner = data::Scanner::get(DataFormat::BUFR);

        {
            metadata::TestCollection mdc("inbound/synop-gts.bufr");
            int usn;
            wassert_true(scanner->update_sequence_number(mdc[0], usn));
            wassert(actual(usn) == 0);
        }

        {
            metadata::TestCollection mdc("inbound/synop-gts-usn2.bufr");
            int usn;
            wassert_true(scanner->update_sequence_number(mdc[0], usn));
            wassert(actual(usn) == 2);
        }
    });
}

} // namespace
