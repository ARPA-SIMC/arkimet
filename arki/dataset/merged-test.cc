#include "arki/dataset/tests.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/session.h"
#include "arki/dataset/pool.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/stream.h"
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
    std::shared_ptr<dataset::Pool> pool;
    std::shared_ptr<dataset::merged::Dataset> ds;
    std::shared_ptr<core::cfg::Sections> config;

    Fixture()
    {
        // In-memory dataset configuration
        string conf =
            "[test200]\n"
            "type = iseg\n"
            "format = grib\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "index = origin, reftime\n"
            "name = test200\n"
            "path = test200\n"
            "\n"
            "[test80]\n"
            "type = iseg\n"
            "format = grib\n"
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
        pool = std::make_shared<dataset::Pool>(session);
        // Cleanup the test datasets
	sys::rmtree_ifexists("test200");
	sys::rmtree_ifexists("test80");
	sys::rmtree_ifexists("error");

        // Import data into the datasets
        metadata::TestCollection mdc("inbound/test.grib1");
        wassert(import("test200", mdc[0]));
        wassert(import("test80", mdc[1]));
        wassert(import("error", mdc[2]));
        pool->add_dataset(*config->section("test200"));
        pool->add_dataset(*config->section("test80"));
        pool->add_dataset(*config->section("error"));

        ds = std::make_shared<dataset::merged::Dataset>(pool);
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


struct TestProgress : public query::Progress
{
    using query::Progress::count;
    using query::Progress::bytes;
    unsigned start_called = 0;
    unsigned update_called = 0;
    unsigned done_called = 0;

    void start(size_t expected_count=0, size_t expected_bytes=0) override
    {
        query::Progress::start(expected_count, expected_bytes);
        ++start_called;
    }
    void update(size_t count, size_t bytes) override
    {
        query::Progress::update(count, bytes);
        ++update_called;
    }
    void done() override
    {
        query::Progress::done();
        ++done_called;
    }
};

struct TestProgressThrowing : public TestProgress
{
    void update(size_t count, size_t bytes) override
    {
        TestProgress::update(count, bytes);
        throw std::logic_error("Expected error");
    }
};


void Tests::register_tests() {

// Test querying the datasets
add_method("query", [](Fixture& f) {
    metadata::Collection mdc(*f.ds, Matcher());
    wassert(actual(mdc.size()) == 3u);
});

add_method("progress_query_data", [](Fixture& f) {
    auto reader = f.ds->create_reader();

    auto progress = make_shared<TestProgress>();

    query::Data dq;
    dq.progress = progress;
    size_t count = 0;
    reader->query_data(dq, [&](std::shared_ptr<Metadata> md) noexcept { ++count; return true; });
    wassert(actual(count) == 3u);
    wassert(actual(progress->count) == 3u);
    wassert(actual(progress->bytes) == 44412u);
    wassert(actual(progress->start_called) == 1u);
    wassert(actual(progress->update_called) == 3u);
    wassert(actual(progress->done_called) == 1u);
});

add_method("progress_query_bytes", [](Fixture& f) {
    auto reader = f.ds->create_reader();
    auto progress = make_shared<TestProgress>();
    query::Bytes bq;
    bq.progress = progress;
    auto out = StreamOutput::create_discard();
    reader->query_bytes(bq, *out);
    wassert(actual(progress->count) == 3u);
    wassert(actual(progress->bytes) == 44412u);
    wassert(actual(progress->start_called) == 1u);
    wassert(actual(progress->update_called) == 3u);
    wassert(actual(progress->done_called) == 1u);
});

add_method("progress_query_data_throws", [](Fixture& f) {
    auto reader = f.ds->create_reader();
    auto progress1 = make_shared<TestProgressThrowing>();
    
    query::Data dq;
    dq.progress = progress1;
    size_t count = 0;
    auto e = wassert_throws(std::logic_error, reader->query_data(dq, [&](std::shared_ptr<Metadata> md) noexcept { ++count; return true; }));
    wassert(actual(e.what()) = "Expected error");
});

add_method("progress_query_bytes_throws", [](Fixture& f) {
    auto reader = f.ds->create_reader();
    auto out = StreamOutput::create_discard();
    auto progress1 = make_shared<TestProgressThrowing>();
    query::Bytes bq;
    bq.progress = progress1;
    auto e = wassert_throws(std::logic_error, reader->query_bytes(bq, *out));
    wassert(actual(e.what()) = "Expected error");
});

}

}
