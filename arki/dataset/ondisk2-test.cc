#include "arki/dataset/tests.h"
#include "arki/exceptions.h"
#include "arki/dataset/query.h"
#include "arki/dataset/ondisk2/reader.h"
#include "arki/dataset/ondisk2/writer.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/matcher.h"
#include "arki/scan.h"
#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include "arki/types/reftime.h"
#include <unistd.h>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::types;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=ondisk2
            step=daily
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
};

Tests tests("arki_dataset_ondisk2");


void Tests::register_tests() {

// Test acquiring data
add_method("acquire", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");

    auto writer = f.makeOndisk2Writer();

    // Import once in the empty dataset
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_OK);

    // See if we catch duplicate imports
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_ERROR_DUPLICATE);

    // Flush the changes and check that everything is allright
    writer->flush();

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/index.sqlite").exists());
});

// Test replacing an element
add_method("replace", [](Fixture& f) {
    f.import();

    metadata::Collection mdc = f.query(dataset::DataQuery("origin:GRIB1,80", true));
    wassert(actual(mdc.size()) == 1u);

    // Take note of the original source
    unique_ptr<source::Blob> blob1(mdc[0].sourceBlob().clone());

    // Reimport, replacing
    {
        auto writer = f.makeOndisk2Writer();
        if (writer->acquire(mdc[0], dataset::REPLACE_ALWAYS) != dataset::ACQ_OK)
        {
            std::vector<Note> notes = mdc[0].notes();
            for (size_t i = 0; i < notes.size(); ++i)
                cerr << " md note: " << notes[i] << endl;
            wassert(throw TestFailed("acquire result was not ACQ_OK"));
        }
        writer->flush();
    }

    // Fetch the element again
    mdc = f.query(Matcher::parse("origin:GRIB1,80"));
    wassert(actual(mdc.size()) == 1u);

    // Get the new source
    unique_ptr<source::Blob> blob2(mdc[0].sourceBlob().clone());

    // Ensure that it's on the same file (since the reftime did not change)
    wassert(actual(blob1->filename) == blob2->filename);

    // Ensure that it's on a different position
    wassert(actual(blob1->offset) < blob2->offset);

    // Ensure that it's the same length
    wassert(actual(blob1->size) == blob2->size);
});

// Test removing an element
add_method("remove", [](Fixture& f) {
    f.import();

    metadata::Collection mdc = f.query(Matcher::parse("origin:GRIB1,200"));

    // Check that it has a source and metadata element
    wassert(actual(mdc[0].has_source_blob()).istrue());

    // Remove it
    {
        auto writer = f.makeOndisk2Writer();
        writer->remove(mdc[0]);
        writer->flush();
    }

    // Check that it does not have a source and metadata element
    wassert(actual(mdc[0].has_source()).isfalse());

    // Try to fetch the element again
    mdc = f.query(Matcher::parse("origin:GRIB1,200"));
    wassert(actual(mdc.size()) == 0u);
});

// Test querying the summary
add_method("query_summary", [](Fixture& f) {
    f.import();
    auto reader = f.makeOndisk2Reader();
    Summary summary;
    reader->query_summary(Matcher::parse("origin:GRIB1,200"), summary);
    wassert(actual(summary.count()) == 1u);
});

// Test querying the summary by reftime
add_method("query_summary_reftime", [](Fixture& f) {
    f.import();
    auto reader = f.makeOndisk2Reader();
    Summary summary;
    reader->query_summary(Matcher::parse("reftime:>=2007-10"), summary);
    wassert(actual(summary.count()) == 1u);
});

// Test acquiring data with replace=1
add_method("acquire_replace", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");

    // Import once
    {
        auto writer = f.makeOndisk2Writer();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_OK);
        writer->flush();
    }

    // Import again, make sure they're all duplicates
    {
        auto writer = f.makeOndisk2Writer();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_ERROR_DUPLICATE);
        writer->flush();
    }

    // Import again with replace=true, make sure they're all ok
    {
        auto cfg(f.cfg);
        cfg.set("replace", "true");
        auto dataset = std::make_shared<dataset::ondisk2::Dataset>(f.session(), cfg);
        auto writer = dataset->create_writer();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_OK);
        writer->flush();
    }

    // Make sure all the data files need repack, as they now have got deleted data inside
    //ensure(hasPackFlagfile("test200/2007/07-08.grib"));
    //ensure(hasPackFlagfile("test200/2007/07-07.grib"));
    //ensure(hasPackFlagfile("test200/2007/10-09.grib"));

    // Test querying the dataset
    {
        metadata::Collection mdc = f.query(Matcher());
        wassert(actual(mdc.size()) == 3u);

        // Make sure we're not getting the deleted element
        const source::Blob& blob0 = mdc[0].sourceBlob();
        wassert(actual(blob0.offset) > 0u);
        const source::Blob& blob1 = mdc[1].sourceBlob();
        wassert(actual(blob1.offset) > 0u);
        const source::Blob& blob2 = mdc[2].sourceBlob();
        wassert(actual(blob2.offset) > 0u);
    }

    // Test querying the summary
    {
        auto reader = f.makeOndisk2Reader();
        Summary summary;
        reader->query_summary(Matcher(), summary);
        wassert(actual(summary.count()) == 3u);
    }
});

// Test querying the first reftime extreme of the summary
add_method("query_first_reftime_extreme", [](Fixture& f) {
    f.import();
    auto reader = f.makeOndisk2Reader();
    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    unique_ptr<Reftime> rt = summary.getReferenceTime();
    wassert(actual(rt->style()) == Reftime::Style::PERIOD);
    unique_ptr<reftime::Period> p = downcast<reftime::Period>(move(rt));
    metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,80; reftime:=" + p->begin.to_iso8601()));
    wassert(actual(mdc.size()) == 1u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,98; reftime:=" + p->end.to_iso8601()));
    wassert(actual(mdc.size()) == 1u);
});

// Test acquiring data on a compressed file
add_method("acquire_compressed", [](Fixture& f) {
    f.cfg.set("step", "yearly");

    metadata::TestCollection mdc("inbound/test.grib1");

    // Import the first
    {
        auto writer = f.makeOndisk2Writer();
        wassert(actual(*writer).import(mdc[0]));
    }
    wassert(actual_file("testds/20/2007.grib").exists());

    // Compress what is imported so far
    {
        auto checker = f.makeSegmentedChecker();
        checker->segment("20/2007.grib")->compress(512);
    }
    wassert(actual_file("testds/20/2007.grib").not_exists());

    // Import the second
    {
        auto writer = f.makeOndisk2Writer();
        try {
            writer->acquire(mdc[1]);
            wassert(actual(false).istrue());
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("cannot write to .gz segments"));
        }
    }
    wassert(actual_file("testds/20/2007.grib").not_exists());
});

// Test Update Sequence Number replacement strategy
add_method("acquire_replace_usn", [](Fixture& f) {
    auto writer = f.makeOndisk2Writer();

    metadata::TestCollection mdc("inbound/synop-gts.bufr");
    metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

    wassert(actual(mdc.size()) == 1u);

    // Acquire
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_OK);

    // Acquire again: it fails
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire again: it fails even with a higher USN
    wassert(actual(writer->acquire(mdc_upd[0])) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire with replace: it works
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_ALWAYS)) == dataset::ACQ_OK);

    // Acquire with USN: it works, since USNs the same as the existing ones do overwrite
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Acquire with a newer USN: it works
    wassert(actual(writer->acquire(mdc_upd[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Acquire with the lower USN: it fails
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire with the same high USN: it works, since USNs the same as the existing ones do overwrite
    wassert(actual(writer->acquire(mdc_upd[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Try to query the element and see if it is the right one
    {
        metadata::Collection mdc_read = f.query(dataset::DataQuery("origin:BUFR", true));
        wassert(actual(mdc_read.size()) == 1u);
        int usn;
        wassert(actual(scan::Scanner::update_sequence_number(mdc_read[0], usn)).istrue());
        wassert(actual(usn) == 2);
    }
});

}
}
