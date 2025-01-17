#include "tests.h"
#include "arki/core/lock.h"
#include "arki/query.h"
#include "arki/matcher/parser.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "metadata.h"
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

Tests<GRIBData> test_grib("arki_segment_metadata_grib");
Tests<BUFRData> test_bufr("arki_segment_metadata_bufr");
Tests<VM2Data> test_vm2("arki_segment_metadata_vm2");
Tests<ODIMData> test_odim("arki_segment_metadata_odim");
Tests<NCData> test_netcdf("arki_segment_metadata_netcdf");
Tests<JPEGData> test_jpeg("arki_segment_metadata_jpeg");

std::shared_ptr<segment::metadata::Reader> make_reader(TestData& td, const char* name="test/test")
{
    auto session = std::make_shared<segment::Session>(".");
    auto segment = session->segment_from_relpath(sys::with_suffix(name, "." + format_name(td.format)));
    fill_metadata_segment(segment, td.mds);
    auto lock = std::make_shared<core::lock::NullReadLock>();
    return std::make_shared<segment::metadata::Reader>(segment, lock);

}

template<typename Data>
void Tests<Data>::register_tests() {

add_method("read_all", [] {
    Data td;
    auto reader = make_reader(td);
    metadata::Collection all;
    wassert_true(reader->read_all(all.inserter_func()));
    wassert_true(all == td.mds);
    wassert(actual(all[0].sourceBlob().basedir) == reader->segment().root());
    wassert(actual(all[0].sourceBlob().filename) == "test/test." + format_name(td.format));
});

add_method("query_data_unlocked", [] {
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

add_method("query_data_locked", [] {
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

add_method("query_summary_from_metadata", [] {
    Data td;
    auto reader = make_reader(td);
    wassert(actual(reader->segment()).has_data());
    wassert(actual(reader->segment()).has_metadata());
    wassert(actual(reader->segment()).not_has_summary());

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    summary.clear();
    matcher::Parser parser;
    reader->query_summary(parser.parse("reftime:=2007-07-08"), summary);
    wassert(actual(summary.count()) == 1u);

});

add_method("query_summary_from_summary", [] {
    Data td;
    auto reader = make_reader(td);
    {
        Summary s;
        td.mds.add_to_summary(s);
        s.writeAtomically(reader->segment().abspath_summary());
    }
    wassert(actual(reader->segment()).has_data());
    wassert(actual(reader->segment()).has_metadata());
    wassert(actual(reader->segment()).has_summary());

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    summary.clear();
    matcher::Parser parser;
    reader->query_summary(parser.parse("reftime:=2007-07-08"), summary);
    switch (td.format)
    {
        case DataFormat::NETCDF:
        case DataFormat::JPEG:
            // Current NetCDF and JPEG fixtures do not have enough metadata for
            // the summary to tell them apart enough to have a difference with
            // this matcher
            wassert(actual(summary.count()) == 3u);
            break;
        default:
            wassert(actual(summary.count()) == 1u);
            break;
    }

});

add_method("metadata_without_data", [] {
    Data td;
    auto reader = make_reader(td);
    if (std::filesystem::is_directory(reader->segment().abspath()))
        sys::rmtree_ifexists(reader->segment().abspath());
    else
        std::filesystem::remove(reader->segment().abspath());
    wassert(actual(reader->segment()).not_has_data());
    wassert(actual(reader->segment()).has_metadata());
    wassert(actual(reader->segment()).not_has_summary());

    // Reads succeed until data is actually accessed.
    // In reality thigs should fail when trying to lock the segment

    reader->read_all([](auto) noexcept {return true;});
    reader->query_data(Matcher(), [](auto) noexcept {return true;});
    reader->query_data(query::Data(Matcher(), true), [](auto) noexcept {return true;});
    Summary summary;
    reader->query_summary(Matcher(), summary);
});


add_method("data_without_metadata", [] {
    Data td;
    auto reader = make_reader(td);
    std::filesystem::remove(reader->segment().abspath_metadata());
    wassert(actual(reader->segment()).has_data());
    wassert(actual(reader->segment()).not_has_metadata());
    wassert(actual(reader->segment()).not_has_summary());

    wassert_throws(std::system_error, reader->read_all([](auto) noexcept {return true;}));
    wassert_throws(std::system_error, reader->query_data(Matcher(), [](auto) noexcept {return true;}));
    Summary summary;
    wassert_throws(std::system_error, reader->query_summary(Matcher(), summary));
});


}

}

