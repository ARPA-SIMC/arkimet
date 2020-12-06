#include "arki/core/tests.h"
#include "libarchive.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
    void setup() override {
        TestCase::setup();
        skip_unless_libarchive();
    }
} test("arki_metadata_libarchive");

void Tests::register_tests() {

add_method("tar", [] {
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.tar", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("tar", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("tar tf test.tar > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("targz", [] {
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.tar.gz", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("tar.gz", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("tar ztf test.tar.gz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("tarxz", [] {
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.tar.xz", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("tar.xz", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("tar Jtf test.tar.xz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "data/000001.grib\n"
        "data/000002.grib\n"
        "data/000003.grib\n"
        "data/metadata.md\n"
    );
});

add_method("zip", [] {
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.zip", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("zip", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

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
