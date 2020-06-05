#include "arki/dataset/tests.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/query.h"
#include "arki/dataset/session.h"
#include "arki/dataset/progress.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    std::shared_ptr<dataset::Session> session;
    std::shared_ptr<dataset::merged::Dataset> ds;
    std::shared_ptr<core::cfg::Sections> config;

    Fixture()
    {
        // In-memory dataset configuration
        string conf =
            "[test200]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "index = origin, reftime\n"
            "name = test200\n"
            "path = test200\n"
            "\n"
            "[test80]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,80\n"
            "index = origin, reftime\n"
            "name = test80\n"
            "path = test80\n"
            "\n"
            "[error]\n"
            "type = error\n"
            "step = daily\n"
            "name = error\n"
            "path = error\n";
        config = core::cfg::Sections::parse(conf, "(memory)");
    }

    void test_setup()
    {
        session = std::make_shared<dataset::Session>();
        // Cleanup the test datasets
        system("rm -rf test200/*");
        system("rm -rf test80/*");
        system("rm -rf error/*");

        // Import data into the datasets
        metadata::TestCollection mdc("inbound/test.grib1");
        wassert(import("test200", mdc[0]));
        wassert(import("test80", mdc[1]));
        wassert(import("error", mdc[2]));
        session->add_dataset(*config->section("test200"));
        session->add_dataset(*config->section("test80"));
        session->add_dataset(*config->section("error"));

        ds = std::make_shared<dataset::merged::Dataset>(session);
    }

    void import(const std::string& dsname, Metadata& md)
    {
        auto writer = session->dataset(*config->section(dsname))->create_writer();
        wassert(arki::tests::actual(writer.get()).import(md));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_merged");

void Tests::register_tests() {

// Test querying the datasets
add_method("query", [](Fixture& f) {
    metadata::Collection mdc(*f.ds, Matcher());
    wassert(actual(mdc.size()) == 3u);
});

add_method("progess", [](Fixture& f) {
    auto reader = f.ds->create_reader();

    struct TestProgress : public dataset::QueryProgress
    {
        using dataset::QueryProgress::count;
        using dataset::QueryProgress::bytes;
        unsigned start_called = 0;
        unsigned update_called = 0;
        unsigned done_called = 0;

        void start(size_t expected_count=0, size_t expected_bytes=0) override
        {
            QueryProgress::start(expected_count, expected_bytes);
            ++start_called;
        }
        void update(size_t count, size_t bytes) override
        {
            QueryProgress::update(count, bytes);
            ++update_called;
        }
        void done() override
        {
            QueryProgress::done();
            ++done_called;
        }
    };
    auto progress = make_shared<TestProgress>();

    dataset::DataQuery dq;
    dq.progress = progress;
    size_t count = 0;
    reader->query_data(dq, [&](std::shared_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 3u);
    wassert(actual(progress->count) == 3u);
    wassert(actual(progress->bytes) == 44412u);
    wassert(actual(progress->start_called) == 1u);
    wassert(actual(progress->update_called) == 3u);
    wassert(actual(progress->done_called) == 1u);


    progress = make_shared<TestProgress>();
    dataset::ByteQuery bq;
    bq.progress = progress;
    sys::File out("/dev/null", O_WRONLY);
    reader->query_bytes(bq, out);
    wassert(actual(progress->count) == 3u);
    wassert(actual(progress->bytes) == 44412u);
    wassert(actual(progress->start_called) == 1u);
    wassert(actual(progress->update_called) == 3u);
    wassert(actual(progress->done_called) == 1u);


    struct TestProgressThrowing : public TestProgress
    {
        void update(size_t count, size_t bytes) override
        {
            TestProgress::update(count, bytes);
            throw std::runtime_error("Expected error");
        }
    };

    auto progress1 = make_shared<TestProgressThrowing>();
    dq.progress = progress1;
    count = 0;
    auto e = wassert_throws(std::runtime_error, reader->query_data(dq, [&](std::shared_ptr<Metadata> md) { ++count; return true; }));
    wassert(actual(e.what()) = "Expected error");

    progress1 = make_shared<TestProgressThrowing>();
    bq.progress = progress1;
    e = wassert_throws(std::runtime_error, reader->query_bytes(bq, out));
    wassert(actual(e.what()) = "Expected error");
});

}

}
