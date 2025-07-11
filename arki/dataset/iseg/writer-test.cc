#include "arki/dataset/iseg/reader.h"
#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/tests.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

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
    if (!md.has_source_blob())
        return "(md source is not a blob source)";
    return md.sourceBlob().basedir.filename();
}

struct Fixture : public DatasetTest
{
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
    void clean_and_import(const std::string& testfile = "inbound/test.grib1")
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

void Tests::register_tests()
{

    // Add here only iseg-specific tests that are not convered by tests in
    // dataset-writer-test.cc

    add_method("acquire", [](Fixture& f) {
        metadata::TestCollection mdc("inbound/test.grib1");
        auto md = mdc.get(0);

        auto writer = f.makeIsegWriter();

        // Import once in the empty dataset
        wassert(actual(*writer).acquire_ok(md));
#if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
#endif
        wassert(actual(dsname(*md)) == "testds");

        wassert(actual_type(md->source())
                    .is_source_blob(DataFormat::GRIB,
                                    std::filesystem::canonical("./testds"),
                                    "2007/07-08.grib", 0, 7218));

        // Import again finds the duplicate
        wassert(actual(writer).acquire_duplicate(md));

        // Flush the changes and check that everything is allright
        writer->flush();
        wassert(actual_file("testds/2007/07-08.grib").exists());
        wassert(actual_file("testds/2007/07-08.grib.index").exists());
        wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
        wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
        wassert(actual_file("testds/.summaries/2007-08.summary").not_exists());
        wassert(actual_file("testds/.summaries/all.summary").not_exists());
        wassert(actual_file("testds/MANIFEST").not_exists());
        wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <=
                sys::timestamp("testds/2007/07-08.grib.index"));
        wassert_false(files::hasDontpackFlagfile("testds"));

        f.import_results.acquire(md->clone());

        wassert(f.query_results({0}));
        wassert(f.all_clean(1));
    });

    add_method("acquire_invalidates_summary", [](Fixture& f) {
        f.import_results.clear();
        metadata::TestCollection mdc("inbound/test.grib1");
        // Import an item and query the whole summary: summaries are generated
        {
            auto md0    = mdc.get(0);
            auto md2    = mdc.get(2);
            auto writer = f.makeIsegWriter();
            wassert(actual(*writer).acquire_ok(md0));
            wassert(actual(*writer).acquire_ok(md2));
            writer->flush();
            f.import_results.acquire(md0->clone());
            f.import_results.acquire(md2->clone());
        }
        wassert(f.query_results({0, 1}));
        wassert(f.all_clean(2));
        {
            Summary summary;
            f.makeIsegReader()->query_summary(Matcher(), summary);
        }

        wassert(actual_file("testds/2007/07-08.grib").exists());
        wassert(actual_file("testds/2007/07-08.grib.index").exists());
        wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
        wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
        wassert(actual_file("testds/2007/10-09.grib").exists());
        wassert(actual_file("testds/2007/10-09.grib.index").exists());
        wassert(actual_file("testds/2007/10-09.grib.metadata").not_exists());
        wassert(actual_file("testds/2007/10-09.grib.summary").not_exists());
        wassert(actual_file("testds/.summaries/2007-07.summary").exists());
        wassert(actual_file("testds/.summaries/2007-08.summary").exists());
        wassert(actual_file("testds/.summaries/2007-09.summary").exists());
        wassert(actual_file("testds/.summaries/2007-10.summary").exists());
        wassert(actual_file("testds/.summaries/all.summary").exists());
        wassert(actual_file("testds/MANIFEST").not_exists());

        // Import a second item: summaries are invalidated
        {
            auto md1    = mdc.get(1);
            auto writer = f.makeIsegWriter();
            wassert(actual(*writer).acquire_ok(md1));

            wassert(actual_file("testds/2007/07-07.grib").exists());
            wassert(actual_file("testds/2007/07-07.grib.index").exists());
            wassert(
                actual_file("testds/2007/07-07.grib.metadata").not_exists());
            wassert(actual_file("testds/2007/07-07.grib.summary").not_exists());
            wassert(actual_file("testds/2007/07-08.grib").exists());
            wassert(actual_file("testds/2007/07-08.grib.index").exists());
            wassert(
                actual_file("testds/2007/07-08.grib.metadata").not_exists());
            wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
            wassert(actual_file("testds/2007/10-09.grib").exists());
            wassert(actual_file("testds/2007/10-09.grib.index").exists());
            wassert(
                actual_file("testds/2007/10-09.grib.metadata").not_exists());
            wassert(actual_file("testds/2007/10-09.grib.summary").not_exists());
            wassert(
                actual_file("testds/.summaries/2007-07.summary").not_exists());
            wassert(actual_file("testds/.summaries/2007-08.summary").exists());
            wassert(actual_file("testds/.summaries/2007-09.summary").exists());
            wassert(actual_file("testds/.summaries/2007-10.summary").exists());
            wassert(actual_file("testds/.summaries/all.summary").not_exists());
            wassert(actual_file("testds/MANIFEST").not_exists());

            writer->flush();
            f.import_results.acquire(md1->clone());
        }

        wassert(f.query_results({2, 0, 1}));
        wassert(f.all_clean(3));
        {
            Summary summary;
            f.makeIsegReader()->query_summary(Matcher(), summary);
        }

        wassert(actual_file("testds/.summaries/2007-07.summary").exists());
        wassert(actual_file("testds/.summaries/all.summary").exists());
    });

    add_method("testacquire", [](Fixture& f) {
        metadata::TestCollection mdc("inbound/test.grib1");
        while (mdc.size() > 1)
            mdc.pop_back();
        auto batch = mdc.make_batch();
        wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[0]->destination) == "testds");

        f.cfg->set("archive age", "1");
        wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::ERROR);
        wassert(actual(batch[0]->destination) == "");

        f.cfg->set("delete age", "1");
        wassert(iseg::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[0]->destination) == "testds");
    });
}

} // namespace
