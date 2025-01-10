#include "fd.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Data, class FixtureData>
class Tests : public SegmentTests<Data, FixtureData>
{
    using SegmentTests<Data, FixtureData>::SegmentTests;
    typedef typename SegmentTests<Data, FixtureData>::Fixture Fixture;
    void register_tests() override;
};

Tests<segment::data::concat::Data, GRIBData> test1("arki_segment_data_concat_grib");
Tests<segment::data::concat::Data, BUFRData> test2("arki_segment_data_concat_bufr");
Tests<segment::data::lines::Data, VM2Data> test3("arki_segment_data_lines_vm2");

template<class Data, class FixtureData>
void Tests<Data, FixtureData>::register_tests() {
SegmentTests<Data, FixtureData>::register_tests();

this->add_method("append", [](Fixture& f) {
    auto session = std::make_shared<segment::Session>();
    auto segment = std::make_shared<Segment>(session, f.td.format, std::filesystem::current_path(),  "testfile." + format_name(f.td.format));
    delete_if_exists(segment->abspath);
    wassert(actual_file(segment->abspath).not_exists());
    {
        segment::data::WriterConfig writer_config;
        auto w = segment->detect_data_writer(writer_config);

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wassert(test_append_transaction_ok(w.get(), f.td.mds[0], Data::padding));

        // Then fail one
        wassert(test_append_transaction_rollback(w.get(), f.td.mds[1], Data::padding));

        // Then succeed again
        wassert(test_append_transaction_ok(w.get(), f.td.mds[2], Data::padding));
    }

    // Data writer goes out of scope, file is closed and flushed
    // Scan the file we created
    metadata::TestCollection mdc1;
    wassert(mdc1.scan_from_file(segment->abspath, false));

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(f.td.mds[0]));
    wassert(actual(mdc1[1]).is_similar(f.td.mds[2]));
});

// Test with large files
this->add_method("large", [](Fixture& f) {
    auto session = std::make_shared<segment::Session>();
    auto segment = std::make_shared<Segment>(session, f.td.format, std::filesystem::current_path(),  "testfile." + format_name(f.td.format));
    delete_if_exists(segment->abspath);
    {
        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        segment->detect_data_checker()->test_truncate(0x7FFFFFFF);
        wassert(actual(sys::size(segment->abspath)) == 0x7FFFFFFFu);
    }

    {
        segment::data::WriterConfig writer_config;
        auto dw = segment->detect_data_writer(writer_config);

        // Try a successful transaction
        wassert(test_append_transaction_ok(dw.get(), f.td.mds[0], Data::padding));

        // Then fail one
        wassert(test_append_transaction_rollback(dw.get(), f.td.mds[1], Data::padding));

        // Then succeed again
        wassert(test_append_transaction_ok(dw.get(), f.td.mds[2], Data::padding));
    }

    wassert(actual(sys::size(segment->abspath)) == 0x7FFFFFFFu + f.td.mds[0].data_size() + f.td.mds[2].data_size() + 2 * Data::padding);

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
});

}

}
#include "tests.tcc"
