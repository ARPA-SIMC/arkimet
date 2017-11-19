#include "arki/dataset/tests.h"
#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/reader.h"
#include "arki/dataset/reporter.h"
#include "arki/exceptions.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/grib.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/lock.h"
#include "arki/wibble/sys/childprocess.h"

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
    metadata::Collection mdc("inbound/test.grib1");
    Metadata& md = mdc[0];

    auto writer = f.makeIsegWriter();

    // Import once in the empty dataset
    Writer::AcquireResult res = writer->acquire(md);
    wassert(actual(res) == Writer::ACQ_OK);
    #if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
    #endif
    wassert(actual(dsname(md)) == "testds");

    wassert(actual_type(md.source()).is_source_blob("grib", sys::abspath("./testds"), "2007/07-08.grib", 0, 7218));

    // Import again finds the duplicate
    res = writer->acquire(md);
    wassert(actual(res) == Writer::ACQ_ERROR_DUPLICATE);

    // Flush the changes and check that everything is allright
    writer->flush();
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib.index").exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/" + f.idxfname()).not_exists());
    wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <= sys::timestamp("testds/2007/07-08.grib.index"));
    ensure(!files::hasDontpackFlagfile("testds"));

    f.import_results.push_back(md);

    wassert(f.query_results({0}));
    wassert(f.all_clean(1));
});

add_method("testacquire", [](Fixture& f) {
    metadata::Collection mdc("inbound/test.grib1");
    stringstream ss;
    wassert(actual(iseg::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);

    f.cfg.setValue("archive age", "1");
    wassert(actual(iseg::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_ERROR);

    f.cfg.setValue("delete age", "1");
    wassert(actual(iseg::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);
});

}

}
