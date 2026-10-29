#include "arki/core/file.h"
#include "arki/data/grib.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/metadata/tests.h"
#include "arki/types/area.h"
#include "arki/types/level.h"
#include "arki/types/origin.h"
#include "arki/types/proddef.h"
#include "arki/types/product.h"
#include "arki/types/reftime.h"
#include "arki/types/run.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/timerange.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_data_grib");

class ConcreteScanner : public arki::data::grib::Scanner
{
protected:
    void scan(grib_handle* gh, std::shared_ptr<Metadata> md) override {}
};

void Tests::register_tests()
{
    add_method("scan_file", [] {
        auto basedir = std::filesystem::current_path() / "inbound";
        auto src     = basedir / "fixture.grib1";
        ConcreteScanner scanner;
        std::vector<std::shared_ptr<Metadata>> mds;
        wassert_true(scanner.scan_file(src, [&](std::shared_ptr<Metadata> md_) {
            mds.emplace_back(md_);
            return true;
        }));
        wassert(actual(mds.size()) == 3u);

        wassert(actual_type(mds[0]->source())
                    .is_source_blob(DataFormat::GRIB, basedir, "fixture.grib1",
                                    0, 7218));
        wassert_true(mds[0]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[0]->sourceBlob().read_data());

        wassert(actual_type(mds[1]->source())
                    .is_source_blob(DataFormat::GRIB, basedir, "fixture.grib1",
                                    7218, 34960));
        wassert_true(mds[1]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[1]->sourceBlob().read_data());

        wassert(actual_type(mds[2]->source())
                    .is_source_blob(DataFormat::GRIB, basedir, "fixture.grib1",
                                    42178, 2234));
        wassert_true(mds[2]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[2]->sourceBlob().read_data());
    });

    add_method("scan_pipe", [] {
        auto basedir = std::filesystem::current_path() / "inbound";
        auto src     = basedir / "fixture.grib1";
        ConcreteScanner scanner;
        std::vector<std::shared_ptr<Metadata>> mds;
        core::File in(src, O_RDONLY);
        wassert_true(scanner.scan_pipe(in, [&](std::shared_ptr<Metadata> md) {
            mds.emplace_back(md);
            return true;
        }));
        wassert(actual(mds.size()) == 3u);

        wassert(actual_type(mds[0]->source())
                    .is_source_inline(DataFormat::GRIB, 7218));
        wassert_true(mds[0]->has_cached_data());

        wassert(actual_type(mds[1]->source())
                    .is_source_inline(DataFormat::GRIB, 34960));
        wassert_true(mds[1]->has_cached_data());

        wassert(actual_type(mds[2]->source())
                    .is_source_inline(DataFormat::GRIB, 2234));
        wassert_true(mds[2]->has_cached_data());
    });

    add_method("scan_file_single", [] {
        auto basedir = std::filesystem::current_path() / "inbound";
        auto src     = basedir / "issue264.grib";
        ConcreteScanner scanner;
        std::shared_ptr<Metadata> md = scanner.scan_file_single(src);
        wassert_true(md.get());
        wassert(actual_type(md->source())
                    .is_source_blob(DataFormat::GRIB, basedir, "issue264.grib",
                                    0, 179));
        wassert_true(md->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, md->sourceBlob().read_data());
    });

    add_method("scan_data", [] {
        auto basedir         = std::filesystem::current_path() / "inbound";
        auto src             = basedir / "issue264.grib";
        std::string data_str = sys::read_file(src);
        std::vector<uint8_t> data(data_str.begin(), data_str.end());

        ConcreteScanner scanner;
        std::shared_ptr<Metadata> md = scanner.scan_data(data);
        wassert_true(md.get());
        wassert(actual_type(md->source())
                    .is_source_inline(DataFormat::GRIB, data_str.size()));
        wassert_true(md->has_cached_data());
    });

    // Test validation
    add_method("validation", [] {
        Metadata md;
        vector<uint8_t> buf;

        const data::Validator& v = data::grib::validator();

        sys::File fd("inbound/test.grib1", O_RDONLY);
        v.validate_file(fd, 0, 7218);
        v.validate_file(fd, 7218, 34960);
        v.validate_file(fd, 42178, 2234);

        wassert_throws(std::runtime_error, v.validate_file(fd, 1, 7217));
        wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7217));
        wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7219));
        wassert_throws(std::runtime_error, v.validate_file(fd, 7217, 34961));
        wassert_throws(std::runtime_error, v.validate_file(fd, 42178, 2235));
        wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 0));
        wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 10));

        fd.close();

        metadata::TestCollection mdc("inbound/test.grib1");
        buf = mdc[0].get_data().read();

        wassert(v.validate_buf(buf.data(), buf.size()));
        wassert_throws(
            std::runtime_error,
            v.validate_buf((const char*)buf.data() + 1, buf.size() - 1));
        wassert_throws(std::runtime_error,
                       v.validate_buf(buf.data(), buf.size() - 1));
    });

#if 0
// Check opening very long GRIB files for scanning
// TODO: needs skipping of there are no holes
// FIXME: cannot reenable it because currently there is no interface for
// opening without scanning, and eccodes takes a long time to skip all those
// null bytes
add_method("bigfile", [] {
    data::Grib scanner;
    int fd = open("bigfile.grib1", O_WRONLY | O_CREAT, 0644);
    ensure(fd != -1);
    ensure(ftruncate(fd, 0xFFFFFFFF) != -1);
    close(fd);
    wassert(scanner.test_open("bigfile.grib1"));
});
#endif
}

} // namespace
