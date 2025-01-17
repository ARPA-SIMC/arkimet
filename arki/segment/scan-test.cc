#include "tests.h"
#include "arki/core/lock.h"
#include "arki/query.h"
#include "arki/matcher/parser.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "scan.h"
#include "data.h"
#include <filesystem>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

template<class Data>
class Tests : public TestCase
{
    using TestCase::TestCase;
    using TestCase::add_method;

    void register_tests() override;

    void test_setup() override
    {
        sys::rmtree_ifexists("test");
        std::filesystem::create_directories("test");
    }
};

Tests<GRIBData> test_grib("arki_segment_scan_grib");
Tests<BUFRData> test_bufr("arki_segment_scan_bufr");
Tests<VM2Data> test_vm2("arki_segment_scan_vm2");
Tests<ODIMData> test_odim("arki_segment_scan_odim");
Tests<NCData> test_netcdf("arki_segment_scan_netcdf");
Tests<JPEGData> test_jpeg("arki_segment_scan_jpeg");

std::shared_ptr<segment::scan::Reader> make_reader(TestData& td, const char* name="test/test")
{
    auto session = std::make_shared<segment::Session>(".");
    auto segment = session->segment_from_relpath(sys::with_suffix(name, "." + format_name(td.format)));
    fill_scan_segment(segment, td.mds);
    auto lock = std::make_shared<core::lock::NullReadLock>();
    return std::make_shared<segment::scan::Reader>(segment, lock);

}

template<typename Data>
void Tests<Data>::register_tests() {

add_method("read_all", [&] {
    Data td;
    auto reader = make_reader(td);
    metadata::Collection all;
    wassert_true(reader->read_all(all.inserter_func()));
    wassert(actual(all.size()) == 3u);
    wassert(actual(all[0]).is_similar(td.mds[0]));
    wassert(actual(all[1]).is_similar(td.mds[1]));
    wassert(actual(all[2]).is_similar(td.mds[2]));
});

add_method("query_data_unlocked", [&] {
    Data td;
    auto reader = make_reader(td);
    metadata::Collection all;
    matcher::Parser parser;
    query::Data dq(parser.parse("reftime:=2007-07-08"), false);
    wassert_true(reader->query_data(dq, all.inserter_func()));

    wassert(actual(all.size()) == 1u);
    wassert(actual(all[0]).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert_false(all[0].sourceBlob().reader);
});

add_method("query_data_locked", [&] {
    Data td;
    auto reader = make_reader(td);
    metadata::Collection all;
    matcher::Parser parser;
    query::Data dq(parser.parse("reftime:=2007-07-08"), true);
    wassert_true(reader->query_data(dq, all.inserter_func()));

    wassert(actual(all.size()) == 1u);
    wassert(actual(all[0]).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert_true(all[0].sourceBlob().reader);
});

add_method("query_summary", [&] {
    Data td;
    auto reader = make_reader(td);
    wassert(actual(reader->segment()).has_data());
    wassert(actual(reader->segment()).not_has_metadata());
    wassert(actual(reader->segment()).not_has_summary());

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    summary.clear();
    matcher::Parser parser;
    reader->query_summary(parser.parse("reftime:=2007-07-08"), summary);
    wassert(actual(summary.count()) == 1u);

});

}

}


