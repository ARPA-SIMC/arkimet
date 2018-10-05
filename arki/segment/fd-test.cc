#include "fd.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    typedef typename SegmentTests<Segment, Data>::Fixture Fixture;
    void register_tests() override;
};

Tests<segment::concat::Segment, GRIBData> test1("arki_segment_concat_grib");
Tests<segment::concat::Segment, BUFRData> test2("arki_segment_concat_bufr");
Tests<segment::lines::Segment, VM2Data> test3("arki_segment_lines_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

this->add_method("append", [](Fixture& f) {
    string relpath = "testfile." + f.td.format;
    string abspath = sys::abspath(relpath);
    sys::unlink_ifexists(relpath);
    wassert(actual_file(relpath).not_exists());
    {
        auto w = Segment::make_writer(f.td.format, sys::getcwd(), relpath, abspath);

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wassert(test_append_transaction_ok(w.get(), f.td.mds[0], Segment::padding));

        // Then fail one
        wassert(test_append_transaction_rollback(w.get(), f.td.mds[1], Segment::padding));

        // Then succeed again
        wassert(test_append_transaction_ok(w.get(), f.td.mds[2], Segment::padding));
    }

    // Data writer goes out of scope, file is closed and flushed
    // Scan the file we created
    metadata::TestCollection mdc1;
    wassert(mdc1.scan_from_file(relpath, false));

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(f.td.mds[0]));
    wassert(actual(mdc1[1]).is_similar(f.td.mds[2]));
});

// Test with large files
this->add_method("large", [](Fixture& f) {
    string relpath = "testfile." + f.td.format;
    string abspath = sys::abspath(relpath);
    sys::unlink_ifexists(relpath);
    {
        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        Segment::make_checker(f.td.format, sys::getcwd(), relpath, abspath)->test_truncate(0x7FFFFFFF);
        wassert(actual(sys::size(relpath)) == 0x7FFFFFFFu);
    }

    {
        auto dw = Segment::make_writer(f.td.format, sys::getcwd(), relpath, abspath);

        // Try a successful transaction
        wassert(test_append_transaction_ok(dw.get(), f.td.mds[0], Segment::padding));

        // Then fail one
        wassert(test_append_transaction_rollback(dw.get(), f.td.mds[1], Segment::padding));

        // Then succeed again
        wassert(test_append_transaction_ok(dw.get(), f.td.mds[2], Segment::padding));
    }

    wassert(actual(sys::size(relpath)) == 0x7FFFFFFFu + f.td.mds[0].data_size() + f.td.mds[2].data_size() + 2 * Segment::padding);

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
});

}

}
#include "tests.tcc"
