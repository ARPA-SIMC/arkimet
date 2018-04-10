#include "arki/tests/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/libarchive.h"
#include "arki/utils/sys.h"
#include "zip.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_zip");

void Tests::register_tests() {

add_method("read", [] {
#ifndef HAVE_LIBZIP
    throw TestSkipped();
#endif

    metadata::TestCollection mds("inbound/fixture.grib1");
    {
        sys::File outfd("test.zip", O_WRONLY | O_TRUNC | O_CREAT);
        metadata::LibarchiveOutput writer("zip", outfd);
        writer.subdir.clear();
        writer.append(mds[0]);
        writer.append(mds[1]);
        writer.append(mds[2]);
        writer.flush();
    }

    sys::File infd("test.zip", O_RDONLY);
    ZipReader reader("grib", std::move(infd));

    auto contents = reader.list_data();
    wassert(actual(contents.size()) == 3u);
    wassert(actual(contents[0].offset) == 1u);
    wassert(actual(contents[0].size) == 7218u);
    wassert(actual(contents[1].offset) == 2u);
    wassert(actual(contents[1].size) == 34960u);
    wassert(actual(contents[2].offset) == 3u);
    wassert(actual(contents[2].size) == 2234u);

    std::vector<uint8_t> data = reader.get(contents[1]);
    wassert_true(data == mds[1].getData());
});

}

}
