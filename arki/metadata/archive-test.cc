#include "arki/core/tests.h"
#include "arki/metadata/archive.h"
#include "arki/metadata/collection.h"
#include "arki/stream.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class BaseTests : public TestCase
{
    using TestCase::TestCase;
    void setup() override {
        TestCase::setup();
        skip_unless_libarchive();
    }

    virtual std::unique_ptr<metadata::ArchiveOutput> create_archive(const std::string& format, const std::string& fname) = 0;

    void register_tests() override;
};

class TestFile : public BaseTests
{
    using BaseTests::BaseTests;

    std::unique_ptr<metadata::ArchiveOutput> create_archive(const std::string& format, const std::string& fname) override
    {
        return metadata::ArchiveOutput::create(format, std::make_shared<sys::File>(fname, O_WRONLY | O_CREAT | O_TRUNC));
    }
} test_file("arki_metadata_archive_file");

class TestStremOutput : public BaseTests
{
    using BaseTests::BaseTests;

    std::unique_ptr<metadata::ArchiveOutput> create_archive(const std::string& format, const std::string& fname) override
    {
        return metadata::ArchiveOutput::create(format, StreamOutput::create(std::make_shared<sys::File>(fname, O_WRONLY | O_CREAT | O_TRUNC)));
    }
} test_streamoutput("arki_metadata_archive_streamoutput");

void BaseTests::register_tests() {

add_method("tar", [&] {
    metadata::TestCollection mds("inbound/test.grib1");
    auto arc_out = create_archive("tar", "test.tar");
    wassert(arc_out->append(mds[0]));
    wassert(arc_out->append(mds[1]));
    wassert(arc_out->append(mds[2]));
    wassert(arc_out->flush(true));

    wassert(actual(system("tar tf test.tar > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("targz", [&] {
    metadata::TestCollection mds("inbound/test.grib1");
    auto arc_out = create_archive("tar.gz", "test.tar.gz");
    wassert(arc_out->append(mds[0]));
    wassert(arc_out->append(mds[1]));
    wassert(arc_out->append(mds[2]));
    wassert(arc_out->flush(true));

    wassert(actual(system("tar ztf test.tar.gz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("tarxz", [&] {
    metadata::TestCollection mds("inbound/test.grib1");
    auto arc_out = create_archive("tar.xz", "test.tar.xz");
    wassert(arc_out->append(mds[0]));
    wassert(arc_out->append(mds[1]));
    wassert(arc_out->append(mds[2]));
    wassert(arc_out->flush(true));

    wassert(actual(system("tar Jtf test.tar.xz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("zip", [&] {
    metadata::TestCollection mds("inbound/test.grib1");
    auto arc_out = create_archive("zip", "test.zip");
    wassert(arc_out->append(mds[0]));
    wassert(arc_out->append(mds[1]));
    wassert(arc_out->append(mds[2]));
    wassert(arc_out->flush(true));

    wassert(actual(system("unzip -l test.zip > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")).matches(
R"(Archive:  test.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
     7218  (2007-07-08|07-08-2007) [0-9]+:00   data/000001.grib
    34960  (2007-07-07|07-07-2007) [0-9]+:00   data/000002.grib
     2234  (2007-10-09|10-09-2007) [0-9]+:00   data/000003.grib
      588  [0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+   data/metadata.md
---------                     -------
    45000                     4 files
)"
    ));
});

}
}
