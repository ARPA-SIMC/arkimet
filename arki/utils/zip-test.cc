#include "arki/core/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/archive.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/segment/data.h"
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

void Tests::register_tests()
{

    add_method("read", [] {
        skip_unless_libzip();

        metadata::TestCollection mds("inbound/fixture.grib1");
        {
            auto writer = metadata::ArchiveOutput::create_file(
                "zip", std::make_shared<sys::File>(
                           "test.zip", O_WRONLY | O_TRUNC | O_CREAT));
            wassert(writer->set_subdir(std::string()));
            wassert(writer->append(mds[0]));
            wassert(writer->append(mds[1]));
            wassert(writer->append(mds[2]));
            wassert(writer->flush(true));
        }

        sys::File infd("test.zip", O_RDONLY);
        ZipReader reader(DataFormat::GRIB, std::move(infd));

        auto contents = reader.list_data();
        wassert(actual(contents.size()) == 3u);
        wassert(actual(contents[0].offset) == 1u);
        wassert(actual(contents[0].size) == 7218u);
        wassert(actual(contents[1].offset) == 2u);
        wassert(actual(contents[1].size) == 34960u);
        wassert(actual(contents[2].offset) == 3u);
        wassert(actual(contents[2].size) == 2234u);

        std::vector<uint8_t> data = reader.get(contents[1]);
        wassert_true(data == mds[1].get_data().read());
    });
}

} // namespace
