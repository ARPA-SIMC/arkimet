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

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;

    void test_setup() override
    {
        sys::rmtree_ifexists("test");
        std::filesystem::create_directories("test");
    }
} test("arki_segment_metadata");

std::shared_ptr<segment::metadata::Reader> make_reader(TestData& td, const char* name="test/test")
{
    auto session = std::make_shared<segment::Session>(".");
    auto segment = session->segment_from_relpath(sys::with_suffix(name, "." + format_name(td.format)));
    fill_metadata_segment(segment, td.mds);
    auto lock = std::make_shared<core::lock::NullReadLock>();
    return std::make_shared<segment::metadata::Reader>(segment, lock);

}

void Tests::register_tests() {

add_method("read_all", [] {
    GRIBData td;
    auto reader = make_reader(td);
    metadata::Collection all;
    wassert_true(reader->read_all(all.inserter_func()));
    wassert_true(all == td.mds);
});

add_method("query_data_unlocked", [] {
    GRIBData td;
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
    GRIBData td;
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
    GRIBData td;
    auto reader = make_reader(td);
    wassert(actual_file("test/test.grib").exists());
    wassert(actual_file("test/test.grib.metadata").exists());
    wassert(actual_file("test/test.grib.summary").not_exists());

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    summary.clear();
    matcher::Parser parser;
    reader->query_summary(parser.parse("reftime:=2007-07-08"), summary);
    wassert(actual(summary.count()) == 1u);

});

add_method("query_summary_from_summary", [] {
    GRIBData td;
    auto reader = make_reader(td);
    {
        Summary s;
        td.mds.add_to_summary(s);
        s.writeAtomically("test/test.grib.summary");
    }
    wassert(actual_file("test/test.grib").exists());
    wassert(actual_file("test/test.grib.metadata").exists());
    wassert(actual_file("test/test.grib.summary").exists());

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 3u);

    summary.clear();
    matcher::Parser parser;
    reader->query_summary(parser.parse("reftime:=2007-07-08"), summary);
    wassert(actual(summary.count()) == 1u);

});

add_method("metadata_without_data", [] {
    GRIBData td;
    auto reader = make_reader(td);
    std::filesystem::remove("test/test.grib");
    wassert(actual_file("test/test.grib").not_exists());
    wassert(actual_file("test/test.grib.metadata").exists());
    wassert(actual_file("test/test.grib.summary").not_exists());

    // Reads succeed until data is actually accessed.
    // In reality thigs should fail when trying to lock the segment

    reader->read_all([](auto) noexcept {return true;});
    reader->query_data(Matcher(), [](auto) noexcept {return true;});
    reader->query_data(query::Data(Matcher(), true), [](auto) noexcept {return true;});
    Summary summary;
    reader->query_summary(Matcher(), summary);
});


add_method("data_without_metadata", [] {
    GRIBData td;
    auto reader = make_reader(td);
    std::filesystem::remove("test/test.grib.metadata");
    wassert(actual_file("test/test.grib").exists());
    wassert(actual_file("test/test.grib.metadata").not_exists());
    wassert(actual_file("test/test.grib.summary").not_exists());

    wassert_throws(std::system_error, reader->read_all([](auto) noexcept {return true;}));
    wassert_throws(std::system_error, reader->query_data(Matcher(), [](auto) noexcept {return true;}));
    Summary summary;
    wassert_throws(std::system_error, reader->query_summary(Matcher(), summary));
});


}

}

