#include "arki/data.h"
#include "arki/matcher/parser.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "iseg/checker.h"
#include "iseg/writer.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = iseg
            step = daily
            format = grib
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_iseg");

void Tests::register_tests()
{

    // Test acquiring data with replace=1
    add_method("acquire_replace", [](Fixture& f) {
        metadata::TestCollection mdc("inbound/test.grib1");

        // Import once
        {
            auto writer = f.makeIsegWriter();
            for (auto& md : mdc)
                wassert(actual(writer).acquire_ok(md));
            writer->flush();
        }

        // Import again, make sure they're all duplicates
        {
            auto writer = f.makeIsegWriter();
            for (auto& md : mdc)
                wassert(actual(writer).acquire_duplicate(md));
            writer->flush();
        }

        // Import again with replace=true, make sure they're all ok
        {
            auto cfg(f.cfg);
            cfg->set("replace", "true");
            auto config =
                std::make_shared<dataset::iseg::Dataset>(f.session(), *cfg);
            auto writer = config->create_writer();
            for (auto& md : mdc)
                wassert(actual(writer).acquire_ok(md));
            writer->flush();
        }

        // Test querying the dataset
        {
            metadata::Collection mdc = f.query(Matcher());
            wassert(actual(mdc.size()) == 3u);

            // Make sure we're not getting the deleted element
            wassert(actual(mdc[0].sourceBlob().offset) > 0u);
            wassert(actual(mdc[1].sourceBlob().offset) > 0u);
            wassert(actual(mdc[2].sourceBlob().offset) > 0u);
        }
    });

    // Test Update Sequence Number replacement strategy
    add_method("acquire_replace_usn", [](Fixture& f) {
        matcher::Parser parser;
        f.reset_test("type=iseg\nstep=daily\nformat=bufr");
        auto writer = f.makeIsegWriter();

        metadata::TestCollection mdc("inbound/synop-gts.bufr");
        metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

        wassert(actual(mdc.size()) == 1u);

        // Acquire
        wassert(actual(writer).acquire_ok(mdc.get(0)));

        // Acquire again: it fails
        wassert(actual(writer).acquire_duplicate(mdc.get(0)));

        // Acquire again: it fails even with a higher USN
        wassert(actual(writer).acquire_duplicate(mdc_upd.get(0)));

        // Acquire with replace: it works
        wassert(actual(writer).acquire_ok(mdc.get(0), ReplaceStrategy::ALWAYS));

        // Acquire with USN: it works, since USNs the same as the existing ones
        // do overwrite
        wassert(
            actual(writer).acquire_ok(mdc.get(0), ReplaceStrategy::HIGHER_USN));

        // Acquire with a newer USN: it works
        wassert(actual(writer).acquire_ok(mdc_upd.get(0),
                                          ReplaceStrategy::HIGHER_USN));

        // Acquire with the lower USN: it fails
        wassert(actual(writer).acquire_duplicate(mdc.get(0),
                                                 ReplaceStrategy::HIGHER_USN));

        // Acquire with the same high USN: it works, since USNs the same as the
        // existing ones do overwrite
        wassert(actual(writer).acquire_ok(mdc_upd.get(0),
                                          ReplaceStrategy::HIGHER_USN));

        // Try to query the element and see if it is the right one
        {
            metadata::Collection mdc_read =
                f.query(query::Data(parser.parse("origin:BUFR"), true));
            wassert(actual(mdc_read.size()) == 1u);
            auto scanner = data::Scanner::get(mdc_read[0].source().format);
            int usn;
            wassert(actual(scanner->update_sequence_number(mdc_read[0], usn))
                        .istrue());
            wassert(actual(usn) == 2);
        }
    });

    add_method("delete_missing", [](Fixture& f) {
        metadata::TestCollection mdc("inbound/test.grib1");

        // Import once
        {
            auto writer = f.makeIsegWriter();
            wassert(actual(writer).acquire_ok(mdc.get(0)));
            writer->flush();
        }

        // Remove imported segments
        sys::rmtree("testds/2007");

        // Try deleting
        {
            auto checker = f.makeIsegChecker();
            metadata::Collection to_remove;
            to_remove.push_back(mdc[0].clone());
            checker->remove(to_remove);
        }

        // Ensure that this did not accidentally create a segment
        wassert(actual_file("testds/2007/07-08.grib").not_exists());
        wassert(actual_file("testds/2007/07-08.grib.lock").not_exists());
        wassert(actual_file("testds/2007/07-08.grib.index").not_exists());
        wassert(actual_file("testds/2007").not_exists());
    });
}

} // namespace
