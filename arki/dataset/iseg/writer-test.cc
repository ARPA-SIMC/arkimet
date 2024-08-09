#include "arki/dataset/tests.h"
#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/reader.h"
#include "arki/exceptions.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;

namespace {

inline std::string dsname(const Metadata& md)
{
    if (!md.has_source_blob()) return "(md source is not a blob source)";
    return str::basename(md.sourceBlob().basedir);
}

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=iseg
            step=daily
            format=grib
        )");
    }

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1")
    {
        DatasetTest::clean_and_import(testfile);
        wassert(ensure_localds_clean(3, 3));
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_iseg_writer");

void Tests::register_tests() {

// Add here only iseg-specific tests that are not convered by tests in dataset-writer-test.cc

// Test acquiring data
add_method("acquire", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");
    Metadata& md = mdc[0];

    auto writer = f.makeIsegWriter();

    // Import once in the empty dataset
    wassert(actual(*writer).import(md));
    #if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
    #endif
    wassert(actual(dsname(md)) == "testds");

    wassert(actual_type(md.source()).is_source_blob("grib", std::filesystem::canonical("./testds"), "2007/07-08.grib", 0, 7218));

    // Import again finds the duplicate
    wassert(actual(writer->acquire(md)) == ACQ_ERROR_DUPLICATE);

    // Flush the changes and check that everything is allright
    writer->flush();
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib.index").exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/" + f.idxfname()).not_exists());
    wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <= sys::timestamp("testds/2007/07-08.grib.index"));
    wassert_false(files::hasDontpackFlagfile("testds"));

    f.import_results.emplace_back(md.clone());

    wassert(f.query_results({0}));
    wassert(f.all_clean(1));
});

add_method("testacquire", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");
    while (mdc.size() > 1) mdc.pop_back();
    auto batch = mdc.make_import_batch();
    wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    f.cfg->set("archive age", "1");
    wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_ERROR);
    wassert(actual(batch[0]->dataset_name) == "");

    f.cfg->set("delete age", "1");
    wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");
});

}

}
