#include "arki/metadata/tests.h"
#include "arki/exceptions.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/dataset/query.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/types/note.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/summary.h"
#include "arki/iotrace.h"
#include "arki/utils/sys.h"
#include "arki/utils/subprocess.h"
#include "index.h"
#include <memory>
#include <sys/fcntl.h>
#include <unistd.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;
using namespace arki::dataset::index;
using namespace arki::tests;

// Create a dataset index gived its configuration
template<typename INDEX>
inline unique_ptr<WIndex> createIndex(std::shared_ptr<dataset::Session> session, std::shared_ptr<core::Lock> lock, const std::string& text_cfg)
{
    auto cfg = core::cfg::Section::parse(text_cfg);
    auto dataset = std::make_shared<dataset::ondisk2::Dataset>(session, *cfg);
    auto res = unique_ptr<INDEX>(new INDEX(dataset));
    res->lock = lock;
    return res;
}

std::shared_ptr<Metadata> make_md()
{
    auto md = std::make_shared<Metadata>();
    md->set_source(types::Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 10, 2000));
    md->test_set("origin", "GRIB1(200, 10, 100)");
    md->test_set("product", "GRIB1(3, 4, 5)");
    md->test_set("level", "GRIB1(1, 2)");
    md->test_set("timerange", "GRIB1(4, 5s, 6s)");
    md->test_set("reftime", "2006-05-04T03:02:01Z");
    md->test_set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md->test_set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
    md->add_note("this is a test");
    {
        File out("test-md.metadata", O_WRONLY | O_CREAT | O_TRUNC, 0666);
        md->write(out);
        out.close();
    }
    return md;
}

std::shared_ptr<Metadata> make_md1()
{
    auto md1 = std::make_shared<Metadata>();
    md1->set_source(types::Source::createBlobUnlocked("grib", "", "inbound/test-sorted.grib1", 20, 40000));
    md1->test_set("origin", "GRIB1(201, 11, 3)");
    md1->test_set("product", "GRIB1(102, 103, 104)");
    md1->test_set("level", "GRIB1(1, 3)");
    md1->test_set("timerange", "GRIB1(4, 6s, 6s)");
    md1->test_set("reftime", "2003-04-05T06:07:08Z");
    md1->test_set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md1->test_set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
    {
        // Index one without notes
        File out("test-md1.metadata", O_WRONLY | O_CREAT | O_TRUNC, 0666);
        md1->write(out);
        out.close();
    }
    return md1;
}

void query_index(WIndex& idx, const dataset::DataQuery& q, metadata::Collection& dest)
{
    auto session = std::make_shared<dataset::Session>();
    idx.query_data(q, dest.inserter_func());
}

struct ReadHang : public subprocess::Child
{
    core::cfg::Section cfg;

    ReadHang(const std::string& cfgstr)
        : cfg(*core::cfg::Section::parse(cfgstr))
    {
    }

    int main() noexcept override
    {
        try {
            matcher::Parser parser;
            auto session = std::make_shared<dataset::Session>();
            auto config = std::make_shared<dataset::ondisk2::Dataset>(session, cfg);
            auto lock = make_shared<core::lock::Null>();
            RIndex idx(config);
            idx.lock = lock;
            idx.open();
            idx.query_data(parser.parse("origin:GRIB1"), [&](std::shared_ptr<Metadata> md) {
                fputs("H\n", stdout);
                fflush(stdout);
                fclose(stdout);
                usleep(100000);
                return true;
            });
        } catch (std::exception& e) {
            fprintf(stderr, "%s\n", e.what());
            fputs("E\n", stdout);
            return 1;
        }
        return 0;
    }

    void start()
    {
        set_stdout(subprocess::Redirect::PIPE);
        fork();
    }

    char waitUntilHung()
    {
        char buf[2];
        ssize_t res = read(get_stdout(), buf, 1);
        if (res == 0)
            throw std::runtime_error("reader did not produce data");
        if (res != 1)
            throw_system_error("cannot read 1 byte from child process");
        return buf[0];
    }
};


class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_ondisk2_index");

void Tests::register_tests() {

// Trying indexing a few metadata
add_method("index", [] {
    matcher::Parser parser;
    auto md = make_md();
    auto md1 = make_md1();

    auto lock = make_shared<core::lock::Null>();
    auto session = std::make_shared<dataset::Session>();
    unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
        "type = ondisk2\n"
        "path = .\n"
        "step = daily\n"
        "indexfile = :memory:\n"
        "index = origin, product, level\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
    );
    wassert_true(test.get());
    Pending p;

    test->open();
    p = test->begin_transaction();

    // Index a metadata
    wassert(actual(test->index(*md, "inbound/test.grib1", 0)).isfalse());

    // Index a second one
    wassert(actual(test->index(*md1, "inbound/test-sorted.grib1", 0)).isfalse());

    // Query various kinds of metadata
    metadata::Collection mdc;
    wassert(query_index(*test, parser.parse("origin:GRIB1,200"), mdc));
    wassert(actual(mdc.size()) == 1u);
    auto notes = mdc[0].notes();
    wassert(actual(notes.second - notes.first) == 1u);
    core::Time time;
    std::string content;
    reinterpret_cast<const types::Note*>(*notes.first)->get(time, content);
    wassert(actual(content) == "this is a test");

    mdc.clear();
    wassert(query_index(*test, parser.parse("product:GRIB1,3"), mdc));
    wassert(actual(mdc.size()) == 1u);

    // TODO: level, timerange, area, proddef, reftime
    p.commit();
});

// See if remove works
add_method("remove", [] {
    matcher::Parser parser;
    auto md = make_md();
    auto md1 = make_md1();

    auto session = std::make_shared<dataset::Session>();
    auto lock = make_shared<core::lock::Null>();
    unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
        "type = ondisk2\n"
        "path = .\n"
        "step = daily\n"
        "indexfile = :memory:\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
        "index = origin, product, level, timerange, area, proddef, reftime\n"
    );
    wassert_true(test.get());
    Pending p;

    test->open();
    p = test->begin_transaction();

    // Index a metadata
    auto res = test->index(*md, "inbound/test.grib1", 0);
    wassert_false(res);
    //int id = test->id(md);

    // Index it again and ensure that it fails
    res = test->index(*md, "inbound/test.grib1", 0);
    wassert_true(res);

    // Index a second one
    test->index(*md1, "inbound/test-sorted.grib1", 0);
    auto pos1 = test->get_current(*md1);

    // Ensure that we have two items
    metadata::Collection mdc;
    query_index(*test, parser.parse("origin:GRIB1"), mdc);
    wassert(actual(mdc.size()) == 2u);
    mdc.clear();

    // Remove a nonexisting item and see that it fails
    wassert_throws(std::runtime_error, test->remove("inbound/test-sorted.grib1", 1));

    // Remove the first item
    wassert(test->remove("inbound/test.grib1", 0));
    wassert(actual(p.pending()).istrue());
    p.commit();

    // There should be only one result now
    query_index(*test, parser.parse("origin:GRIB1"), mdc);
    wassert(actual(mdc.size()) == 1u);

    // It should be the second item we inserted
    auto pos2 = test->get_current(mdc[0]);
    wassert(actual(*pos1) == *pos2);
    mdc.clear();

    // Replace it with a different one
    test->replace(*md1, "inbound/test.grib1", 0);

    // See that it changed
    query_index(*test, parser.parse("origin:GRIB1"), mdc);
    wassert(actual(mdc.size()) == 1u);
    const source::Blob& blob = mdc[0].sourceBlob();
    wassert(actual(blob.filename) == "inbound/test.grib1");

    p.commit();
});

// Test concurrent read and update
add_method("concurrent", [] {
    auto md = make_md();
    auto md1 = make_md1();

    string cfg =
        "type = ondisk2\n"
        "path = .\n"
        "step = daily\n"
        "indexfile = file1\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
        "index = origin, product, level, timerange, area, proddef, reftime\n";

    // Remove index if it exists
    unlink("file1");

    // Create the index and index two metadata
    auto session = std::make_shared<dataset::Session>();
    {
        auto lock = make_shared<core::lock::Null>();
        unique_ptr<WIndex> test1 = createIndex<WIndex>(session, lock, cfg);
        test1->open();

        Pending p = test1->begin_transaction();
        wassert(test1->index(*md, "inbound/test.grib1", 0));
        wassert(test1->index(*md1, "inbound/test-sorted.grib1", 0));
        p.commit();
    }

    // Query the index and hang
    ReadHang readHang(cfg);
    readHang.start();
    wassert(actual(readHang.waitUntilHung()) == 'H');

    // Now try to index another element
    Metadata md3;
    md3.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.bufr", 10, 2000));
    md3.test_set("origin", "GRIB1(202, 12, 102)");
    md3.test_set("product", "GRIB1(3, 4, 5)");
    md3.test_set("level", "GRIB1(1, 2)");
    md3.test_set("timerange", "GRIB1(4, 5s, 6s)");
    md3.test_set("reftime", "2006-05-04T03:02:01Z");
    md3.test_set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md3.test_set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
    md3.add_note("this is a test");
    {
        auto lock = make_shared<core::lock::Null>();
        unique_ptr<WIndex> test1 = createIndex<WIndex>(session, lock, cfg);
        test1->open();
        Pending p = test1->begin_transaction();
        test1->index(md3, "inbound/test.bufr", 0);
        p.commit();
    }

    readHang.send_signal(9);
    readHang.wait();
});

// Test getting the metadata corresponding to a file
add_method("query_file", [] {
    auto md = make_md();
    auto md1 = make_md1();

    // Remove index if it exists
    unlink("file1");

    auto session = std::make_shared<dataset::Session>();
    auto lock = make_shared<core::lock::Null>();
    unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
        "type = ondisk2\n"
        "path = .\n"
        "step = daily\n"
        "indexfile = file1\n"
        "index = origin, product, level\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
    );
    wassert_true(test.get());
    Pending p;

    metadata::TestCollection src("inbound/test.grib1");
    wassert(actual(src.size()) == 3u);

    test->open();
    p = test->begin_transaction();

    // Index two metadata in one file
    test->index(*md, "inbound/padded.grib1", 0);
    test->index(*md1, "inbound/padded.grib1", 10);

    // Index three other metadata in a separate file
    test->index(src[0], "inbound/test.grib1", 0);
    test->index(src[1], "inbound/test.grib1", 10);
    test->index(src[2], "inbound/test.grib1", 20);

    p.commit();

    // Get the metadata corresponding to one file
    metadata::Collection mdc;
    test->scan_file("inbound/padded.grib1", mdc.inserter_func());
    wassert(actual(mdc.size()) == 2u);

    // Check that the metadata came out fine
    mdc[0].set_source(unique_ptr<Source>(md->source().clone()));
    wassert(actual(mdc[0]) == *md);

    mdc[1].set_source(unique_ptr<Source>(md1->source().clone()));
    wassert(actual(mdc[1]) == *md1);
});

// Try a summary query that used to be badly generated
add_method("reproduce_old_issue1", [] {
    matcher::Parser parser;
    auto md = make_md();
    auto md1 = make_md1();

    // Remove index if it exists
    unlink("file1");

    auto session = std::make_shared<dataset::Session>();
    auto lock = make_shared<core::lock::Null>();
    unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
        "type = ondisk2\n"
        "path = \n"
        "step = daily\n"
        "indexfile = file1\n"
        "index = origin, product, level\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
    );
    wassert_true(test.get());
    Pending p;

    test->open();
    p = test->begin_transaction();

    // Index some metadata
    test->index(*md, "test-md", 0);
    test->index(*md1, "test-md1", 0);
    Metadata md2;
    md2.set_source(Source::createBlobUnlocked("grib", "", "inbound/test.bufr", 10, 2000));
    md2.test_set("origin", "GRIB1(202, 12, 102)");
    md2.test_set("product", "GRIB1(3, 4, 5)");
    md2.test_set("level", "GRIB1(1, 2)");
    md2.test_set("timerange", "GRIB1(4, 5s, 6s)");
    md2.test_set("reftime", "2005-01-15T12:00:00Z");
    md2.test_set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md2.test_set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
    test->index(md2, "test-md1", 0);

    p.commit();

    // Query an interval with a partial month only
    Summary summary;
    test->query_summary(parser.parse("reftime:>=2005-01-10,<=2005-01-25"), summary);
    wassert(actual(summary.count()) == 1u);

    // Query an interval with a partial month and a full month
    summary.clear();
    test->query_summary(parser.parse("reftime:>=2004-12-10,<=2005-01-31"), summary);
    wassert(actual(summary.count()) == 1u);

    // Query an interval with a full month and a partial month
    summary.clear();
    test->query_summary(parser.parse("reftime:>=2005-01-01,<=2005-02-15"), summary);
    wassert(actual(summary.count()) == 1u);

    // Query an interval with a partial month, a full month and a partial month
    summary.clear();
    test->query_summary(parser.parse("reftime:>=2004-12-10,<=2005-02-15"), summary);
    wassert(actual(summary.count()) == 1u);
});

// Trying indexing a few metadata in a large file
add_method("largefile", [] {
    auto md = make_md();
    auto md1 = make_md1();

    // Pretend the data is in a very big file
    md->set_source(Source::createBlobUnlocked("grib", "", "inbound/test.grib1", 0x100000000LLU, 2000));
    md1->set_source(Source::createBlobUnlocked("grib", "", "inbound/test-sorted.grib1", 0xFFFFffffFFFF0000LLU, 0xFFFF));

    // Remove index if it exists
    unlink("file1");

    auto session = std::make_shared<dataset::Session>();
    auto lock = make_shared<core::lock::Null>();
    unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
        "type = ondisk2\n"
        "path = .\n"
        "step = daily\n"
        "indexfile = file1\n"
        "index = origin, product, level\n"
        "unique = origin, product, level, timerange, area, proddef, reftime\n"
    );
    wassert_true(test.get());
    Pending p;

    test->open();
    p = test->begin_transaction();

    // Index the two metadata
    test->index(*md, "inbound/test.grib1", md->sourceBlob().offset);
    test->index(*md1, "inbound/test-sorted.grib1", md1->sourceBlob().offset);

    // Query various kinds of metadata
    metadata::Collection mdc;
    query_index(*test, Matcher(), mdc);

    const source::Blob& s0 = mdc[0].sourceBlob();
    wassert(actual(s0.offset) == 0xFFFFffffFFFF0000LLU);
    wassert(actual(s0.size) == 0xFFFFu);

    const source::Blob& s1 = mdc[1].sourceBlob();
    wassert(actual(s1.offset) == 0x100000000LLU);
    wassert(actual(s1.size) == 2000u);

    // TODO: level, timerange, area, proddef, reftime
    p.commit();
});

// Test smallfiles support
add_method("smallfiles", [] {
    skip_unless_vm2();
    auto md = make_md();
    auto md1 = make_md1();

    metadata::TestCollection src("inbound/test.vm2");

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        auto session = std::make_shared<dataset::Session>();
        auto lock = make_shared<core::lock::Null>();
        unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
                "type = ondisk2\n"
                "path = .\n"
                "step = daily\n"
                "indexfile = file1\n"
                "index = origin, product, level\n"
                "unique = origin, product, level, timerange, area, proddef, reftime\n"
        );
        wassert_true(test.get());
        test->open();

        // Insert a metadata
        Pending p = test->begin_transaction();
        test->index(src[0], "inbound/test.vm2", src[0].sourceBlob().offset);
        p.commit();

        // Query it back
        metadata::Collection mdc;
        query_index(*test, Matcher(), mdc);

        // 'value' should not have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", sys::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).is_not_set("value"));

        // I/O should happen here
        mdc[0].drop_cached_data();
        mdc[0].sourceBlob().lock(Segment::detect_reader("vm2", "inbound", "test.vm2", "inbound/test.vm2", std::shared_ptr<core::lock::Null>()));
        const auto& buf = mdc[0].get_data().read();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 1u);
        wassert(actual(collector.events[0].filename).endswith("inbound/test.vm2"));
    }

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        auto session = std::make_shared<dataset::Session>();
        auto lock = make_shared<core::lock::Null>();
        unique_ptr<WIndex> test = createIndex<WIndex>(session, lock,
                "type = ondisk2\n"
                "path = .\n"
                "step = daily\n"
                "indexfile = file1\n"
                "index = origin, product, level\n"
                "unique = origin, product, level, timerange, area, proddef, reftime\n"
                "smallfiles = yes\n"
        );
        wassert_true(test.get());
        test->open();

        // Insert a metadata
        Pending p = test->begin_transaction();
        test->index(src[0], "inbound/test.vm2", src[0].sourceBlob().offset);
        p.commit();

        // Query it back
        metadata::Collection mdc;
        query_index(*test, Matcher(), mdc);

        // 'value' should have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", sys::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).contains("value", "1.2,,,000000000"));

        // No I/O should happen here
        mdc[0].drop_cached_data();
        const auto& buf = mdc[0].get_data().read();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 0u);
    }
});

}

}
