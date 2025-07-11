#include "arki/core/file.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/tests.h"
#include "arki/matcher.h"
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
            type=simple
            step=daily
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
} test("arki_dataset_simple_writer");

void Tests::register_tests()
{

#if 0
// Try to append some data
add_method("segment_append", [] {
    string fname = "testfile.grib";
    string mdfname = "testfile.grib.metadata";
    string sumfname = "testfile.grib.summary";

    std::filesystem::remove(fname);
    ino_t inomd;
    ino_t inosum;

    system(("cp inbound/test.grib1 " + fname).c_str());

    metadata::TestCollection mds("inbound/test.grib1");

    {
        datafile::MdBuf mdbuf("./" + fname, std::shared_ptr<core::lock::Null>());

        // Append the data, source is unchanged
        auto source = types::source::Blob::create_unlocked("grib", "", "test.grib1", 0, datasize(mds[0]));
        wassert(mdbuf.add(mds[0], *source));
        wassert(actual_type(mds[0].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", 0, datasize(mds[0])));

        // Metadata and summaries don't get touched
        wassert(actual_file(mdfname).not_exists());
        wassert(actual_file(sumfname).not_exists());

        // Append another metadata
        source = types::source::Blob::create_unlocked("grib", "", "test.grib1", source->offset + source->size, datasize(mds[1]));
        wassert(mdbuf.add(mds[1], *source));
        wassert(actual_type(mds[1].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", source->offset, datasize(mds[1])));

        // Metadata and summaries don't get touched
        wassert(actual_file(mdfname).not_exists());
        wassert(actual_file(sumfname).not_exists());

        wassert(mdbuf.flush());

        // Metadata and summaries are now there
        wassert(actual_file(mdfname).exists());
        wassert(actual_file(sumfname).exists());
        inomd = sys::inode(mdfname);
        inosum = sys::inode(sumfname);

        // Append another metadata
        source = types::source::Blob::create_unlocked("grib", sys::getcwd(), "inbound/test.grib1", source->offset + source->size, datasize(mds[2]));
        wassert(mdbuf.add(mds[2], *source));
        wassert(actual_type(mds[2].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", source->offset, datasize(mds[2])));

        // Metadata and summaries don't get touched
        wassert(actual(sys::inode(mdfname)) == inomd);
        wassert(actual(sys::inode(sumfname)) == inosum);
    }

    // After Datafile is destroyed, metadata and summaries are flushed
    wassert(actual(sys::inode(mdfname)) != inomd);
    wassert(actual(sys::inode(sumfname)) != inosum);
});
#endif

    // Add here only simple-specific tests that are not convered by tests in
    // dataset-writer-test.cc

    // Test acquiring data
    add_method("acquire", [](Fixture& f) {
        // Clean the dataset
        f.clean();

        metadata::TestCollection mdc("inbound/test.grib1");
        auto md = mdc.get(0);

        auto writer = f.makeSimpleWriter();

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

        // Import again works fine
        wassert(actual(*writer).acquire_ok(md));
        wassert(actual(dsname(*md)) == "testds");

        wassert(actual_type(md->source())
                    .is_source_blob(DataFormat::GRIB,
                                    std::filesystem::canonical("./testds"),
                                    "2007/07-08.grib", 7218, 7218));

        // Flush the changes and check that everything is allright
        writer->flush();
        wassert(actual_file("testds/2007/07-08.grib").exists());
        wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
        wassert(actual_file("testds/2007/07-08.grib.summary").exists());
        wassert(actual_file("testds/MANIFEST").exists());
        wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <=
                sys::timestamp("testds/2007/07-08.grib.metadata"));
        wassert(actual(sys::timestamp("testds/2007/07-08.grib.metadata")) <=
                sys::timestamp("testds/2007/07-08.grib.summary"));
        wassert(actual(sys::timestamp("testds/2007/07-08.grib.summary")) <=
                sys::timestamp("testds/MANIFEST"));
        wassert_true(files::hasDontpackFlagfile("testds"));

        wassert(f.ensure_localds_clean(1, 2));
    });

    // Test appending to existing files
    add_method("append", [](Fixture& f) {
        f.cfg->set("step", "yearly");

        metadata::TestCollection mdc("inbound/test-sorted.grib1");

        // Import once in the empty dataset
        {
            auto writer = f.makeSimpleWriter();
            wassert(actual(*writer).acquire_ok(mdc.get(0)));
        }

        // Import another one, appending to the file
        {
            auto writer = f.makeSimpleWriter();
            wassert(actual(*writer).acquire_ok(mdc.get(1)));
            wassert(actual(dsname(mdc[1])) == "testds");
            wassert(actual_type(mdc[1].source())
                        .is_source_blob(DataFormat::GRIB,
                                        std::filesystem::canonical("testds"),
                                        "20/2007.grib", 34960, 7218));
        }

        wassert(actual_file("testds/20/2007.grib").exists());
        wassert(actual_file("testds/20/2007.grib.metadata").exists());
        wassert(actual_file("testds/20/2007.grib.summary").exists());
        wassert(actual_file("testds/MANIFEST").exists());
        wassert(actual(sys::timestamp("testds/20/2007.grib")) <=
                sys::timestamp("testds/20/2007.grib.metadata"));
        wassert(actual(sys::timestamp("testds/20/2007.grib.metadata")) <=
                sys::timestamp("testds/20/2007.grib.summary"));
        wassert(actual(sys::timestamp("testds/20/2007.grib.summary")) <=
                sys::timestamp("testds/MANIFEST"));

        // Dataset is fine and clean
        wassert(f.ensure_localds_clean(1, 2));
    });

    add_method("testacquire", [](Fixture& f) {
        metadata::TestCollection mdc("inbound/test.grib1");
        while (mdc.size() > 1)
            mdc.pop_back();

        auto batch = mdc.make_batch();
        wassert(simple::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[0]->destination) == "testds");

        f.cfg->set("archive age", "1");
        wassert(simple::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::ERROR);
        wassert(actual(batch[0]->destination) == "");

        f.cfg->set("delete age", "1");
        wassert(simple::Writer::test_acquire(f.session(), *f.cfg, batch));
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[0]->destination) == "testds");
    });
}

} // namespace
