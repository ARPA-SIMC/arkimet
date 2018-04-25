#include "dir.h"
#include "tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
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

const char* relpath = "testfile.grib";

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    void register_tests() override;
};

Tests<segment::dir::Segment, GRIBData> test1("arki_segment_dir_grib");
Tests<segment::dir::Segment, BUFRData> test2("arki_segment_dir_bufr");
Tests<segment::dir::Segment, ODIMData> test3("arki_segment_dir_odim");
Tests<segment::dir::Segment, VM2Data>  test4("arki_segment_dir_vm2");

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
    string abspath = sys::abspath(relpath);
    return std::shared_ptr<segment::dir::Writer>(new segment::dir::Writer("grib", sys::getcwd(), relpath, abspath));
}


template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

// Try to append some data
this->add_method("append", [](Fixture& f) {
    if (sys::isdir(relpath))
        sys::rmtree_ifexists(relpath);
    else
        sys::unlink_ifexists(relpath);
    metadata::TestCollection mdc("inbound/test.grib1");
    wassert(actual_file(relpath).not_exists());
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
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000000.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->segment().relpath, 0, data_size));
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
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000001.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Rollback
            w->rollback();

            // After rollback, the file has been deleted
            wassert(actual(sys::exists(str::joinpath(w->segment().abspath, "000001.grib"))).isfalse());
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
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000002.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->segment().relpath, 2, data_size));
        }
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::TestCollection mdc1;

    // Scan the file we created
    wassert(mdc1.scan_from_file(relpath, false));

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
});

}

}
#include "tests.tcc"
