#include "dir.h"
#include "arki/dataset/segment/tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>
#include <sstream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.write_yaml(o);
    return o;
}
}

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

const char* relname = "testfile.grib";

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment_dir");

inline size_t datasize(const Metadata& md)
{
    return md.data_size();
}

/**
 * Create a writer
 * return the data::Writer so that we manage the writer lifetime, but also
 * the underlying implementation so we can test it.
 */
std::shared_ptr<segment::dir::Writer> make_w()
{
    string absname = sys::abspath(relname);
    return std::shared_ptr<segment::dir::Writer>(new segment::dir::Writer("grib", sys::getcwd(), relname, absname));
}

void Tests::register_tests() {

// Try to append some data
add_method("append", [] {
    sys::rmtree_ifexists(relname);
    metadata::TestCollection mdc("inbound/test.grib1");
    wassert(actual_file(relname).not_exists());
    {
        auto w(make_w());

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        {
            Metadata& md = mdc[0];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            const types::source::Blob& new_source = w->append(md);
            wassert(actual((size_t)new_source.offset) == 0u);
            wassert(actual(sys::size(str::joinpath(w->absname, "000000.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->relname, 0, data_size));
        }


        // Then fail one
        {
            Metadata& md = mdc[1];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            const types::source::Blob& new_source = w->append(md);
            wassert(actual((size_t)new_source.offset) == 1u);
            wassert(actual(sys::size(str::joinpath(w->absname, "000001.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Rollback
            w->rollback();

            // After rollback, the file has been deleted
            wassert(actual(sys::exists(str::joinpath(w->absname, "000001.grib"))).isfalse());
            wassert(actual_type(md.source()) == *orig_source);
        }

        // Then succeed again
        {
            Metadata& md = mdc[2];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            // Rolling back a transaction does leave a gap in the sequence
            const types::source::Blob& new_source = w->append(md);
            wassert(actual((size_t)new_source.offset) == 2u);
            wassert(actual(sys::size(str::joinpath(w->absname, "000002.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->relname, 2, data_size));
        }
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::TestCollection mdc1;

    // Scan the file we created
    wassert(actual(mdc1.scan_from_file(relname, false)).istrue());

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
});

// Common segment tests

add_method("check", [] {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            return std::shared_ptr<segment::Writer>(new segment::dir::Writer(format, root, relname, absname));
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::dir::Checker(format, root, relname, absname));
        }
    } test;

    wassert(test.run());
});

add_method("remove", [] {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            return std::shared_ptr<segment::Writer>(new segment::dir::Writer(format, root, relname, absname));
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::dir::Checker(format, root, relname, absname));
        }
    } test;

    wassert(test.run());
});

}

}
