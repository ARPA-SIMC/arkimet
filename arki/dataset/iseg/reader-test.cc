#include <arki/dataset/tests.h>
#include <arki/dataset/iseg/reader.h>
#include <arki/dataset/iseg/writer.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::dataset::iseg;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = iseg
            step = daily
            postprocess = testcountbytes
            format = grib
        )");

        clean_and_import();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_plain("arki_dataset_iseg_reader");

void Tests::register_tests() {

// Add here only iseg-specific tests that are not convered by tests in dataset-reader-test.cc

add_method("empty", [](Fixture& f) noexcept {
});

}
}
