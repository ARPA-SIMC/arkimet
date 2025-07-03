#include "manifest.h"
#include "arki/core/lock.h"
#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/session.h"
#include "arki/dataset/simple.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/sqlite.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;
using namespace arki::dataset::simple;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;

    void test_setup() override
    {
        if (std::filesystem::exists("testds"))
            sys::rmtree("testds");
        std::filesystem::create_directories("testds");
    }
} test("arki_dataset_simple_manifest");

void fill1(manifest::Writer& writer)
{
    writer.set("2007/07-08.grib", 100, core::Interval(core::Time(2007, 7, 8, 0), core::Time(2007, 7, 8, 18)));
    writer.set("2007/07-07.grib", 100, core::Interval(core::Time(2007, 7, 7, 6), core::Time(2007, 7, 7, 18)));
    writer.set("2007/10-09.grib", 100, core::Interval(core::Time(2007, 10, 9, 0), core::Time(2007, 10, 9, 20)));
}

void create1()
{
    manifest::Writer writer("testds", false);
    fill1(writer);
    writer.flush();
}

void create_sqlite(const std::vector<manifest::SegmentInfo>& contents)
{
    utils::sqlite::SQLiteDB m_db;
    m_db.open("testds/index.sqlite");
    m_db.exec("PRAGMA legacy_file_format = 0");

    // Create the main table
    m_db.exec(R"(
        CREATE TABLE IF NOT EXISTS files (
          id INTEGER PRIMARY KEY,
          file TEXT NOT NULL,
          mtime INTEGER NOT NULL,
          start_time TEXT NOT NULL,
          end_time TEXT NOT NULL,
          UNIQUE(file)
        )
    )");

    utils::sqlite::InsertQuery m_insert(m_db);
    m_insert.compile("INSERT OR REPLACE INTO files (file, mtime, start_time, end_time) VALUES (?, ?, ?, ?)");

    for (const auto& i: contents)
    {
        auto bt = i.time.begin.to_sql();
        auto et = i.time.end.to_sql();
        m_insert.reset();
        m_insert.bind(1, i.relpath.native());
        m_insert.bind(2, i.mtime);
        m_insert.bind(3, bt);
        m_insert.bind(4, et);
        m_insert.step();
    }
}

void Tests::register_tests() {

add_method("exists", [] {
    // Empty dirs do not show up as having a manifest
    wassert_false(manifest::exists("testds"));

    // Plain manifest
    sys::write_file("testds/MANIFEST", "");
    wassert_true(manifest::exists("testds"));

    std::filesystem::remove("testds/MANIFEST");
    wassert_false(manifest::exists("testds"));

    // Legacy sqlite manifest
    sys::write_file("testds/index.sqlite", "");
    wassert_true(manifest::exists("testds"));
});

add_method("empty_reader", [] {
    wassert_false(manifest::exists("testds"));

    manifest::Reader reader("testds");

    reader.reread();
    wassert_false(reader.segment("test.grib1"));

    const auto& file_list_ref = reader.file_list();
    wassert(actual(file_list_ref.size()) == 0u);

    auto file_list = reader.file_list(Matcher());
    wassert(actual(file_list.size()) == 0u);

    wassert(actual(reader.get_stored_time_interval()), core::Interval());
});

add_method("empty_writer", [] {
    core::Interval interval1(core::Time(2024, 1, 1), core::Time(2024, 7, 1));

    wassert_false(manifest::exists("testds"));

    manifest::Writer writer("testds", false);
    wassert(actual_file("testds/MANIFEST").not_exists());

    wassert(writer.remove("does-not-exist"));
    wassert(writer.rename("does-not-exist1", "does-not-exist2"));

    // setting without flush does not create the MANIFEST file
    writer.set("test.grib", 42, interval1);
    wassert(actual_file("testds/MANIFEST").not_exists());

    // data can be read immediately after setting
    const auto& file_list_ref = writer.file_list();
    wassert(actual(file_list_ref.size()) == 1u);
    wassert(actual(file_list_ref[0].relpath) == "test.grib");
    wassert(actual(file_list_ref[0].mtime) == 42);
    wassert(actual(file_list_ref[0].time) == interval1);

    writer.flush();
    wassert(actual_file("testds/MANIFEST").exists());
});

add_method("before_reread", []() {
    manifest::Writer writer("testds", false);
    fill1(writer);
    writer.flush();

    manifest::Reader reader("testds");

    // Reader is empty before reread is called
    wassert(actual(reader.file_list().size()) == 0u);

    reader.reread();
    wassert(actual(reader.file_list().size()) == 3u);
});

add_method("segment", [] {
    create1();
    manifest::Reader reader("testds");
    reader.reread();

    wassert(actual(reader.segment("2007/07-07.grib")).istrue());
    wassert(actual(reader.segment("2007/07-08.grib")).istrue());
    wassert(actual(reader.segment("2007/10-09.grib")).istrue());
    wassert(actual(reader.segment("2007/07-09.grib")).isfalse());

    const auto* s = reader.segment("2007/07-07.grib");
    wassert(actual(s->relpath) == "2007/07-07.grib");
    wassert(actual(s->mtime) == 100);
    wassert(actual(s->time) == core::Interval(core::Time(2007, 7, 7, 6), core::Time(2007, 7, 7, 18)));
});

add_method("file_list", [] {
    create1();
    manifest::Reader reader("testds");
    reader.reread();

    const auto& segs = reader.file_list();

    wassert(actual(segs.size()) == 3u);
    wassert(actual(segs[0].relpath) == "2007/07-07.grib");
    wassert(actual(segs[1].relpath) == "2007/07-08.grib");
    wassert(actual(segs[2].relpath) == "2007/10-09.grib");
});

add_method("file_list_filtered", [] {
    create1();
    manifest::Reader reader("testds");
    reader.reread();

    auto query = [&](const std::string& matcher) {
        matcher::Parser parser;
        Matcher m = parser.parse(matcher);
        return reader.file_list(m);
    };

    auto res = query("reftime:<2007-10-09");
    wassert(actual(res.size()) == 2u);
    wassert(actual(res[0].relpath) == "2007/07-07.grib");
    wassert(actual(res[1].relpath) == "2007/07-08.grib");

    res = query("reftime:<=2007-10-09");
    wassert(actual(res.size()) == 3u);
    wassert(actual(res[0].relpath) == "2007/07-07.grib");
    wassert(actual(res[1].relpath) == "2007/07-08.grib");
    wassert(actual(res[2].relpath) == "2007/10-09.grib");

    res = query("reftime:>2007-07-07");
    wassert(actual(res.size()) == 2u);
    wassert(actual(res[0].relpath) == "2007/07-08.grib");
    wassert(actual(res[1].relpath) == "2007/10-09.grib");

    res = query("reftime:>=2007-07-07");
    wassert(actual(res.size()) == 3u);
    wassert(actual(res[0].relpath) == "2007/07-07.grib");
    wassert(actual(res[1].relpath) == "2007/07-08.grib");
    wassert(actual(res[2].relpath) == "2007/10-09.grib");

    res = query("reftime:=2007-07-08");
    wassert(actual(res.size()) == 1u);
    wassert(actual(res[0].relpath) == "2007/07-08.grib");
});

add_method("add_remove", [] {
    core::Interval interval1(core::Time(2024, 1, 1), core::Time(2024, 7, 1));

    // Test adding and removing files
    manifest::Writer writer("testds", false);
    fill1(writer);

    writer.remove("2007/07-08.grib");
    writer.set("2005/01-01.grib", 42, interval1);

    const auto& segs = writer.file_list();
    wassert(actual(segs.size()) == 3u);
    wassert(actual(segs[0].relpath) == "2005/01-01.grib");
    wassert(actual(segs[1].relpath) == "2007/07-07.grib");
    wassert(actual(segs[2].relpath) == "2007/10-09.grib");
});

add_method("set_mtime", [] {
    manifest::Writer writer("testds", false);
    fill1(writer);

    writer.set_mtime("2007/07-08.grib", 4242);
    wassert_true(writer.is_dirty());

    wassert(actual(writer.segment("2007/07-08.grib")->mtime) == 4242);

    auto e = wassert_throws(std::runtime_error, writer.set_mtime("missing.grib", 1234));
    wassert(actual(e.what()).contains("segment is not in index"));
});

add_method("empty_interval", [] {
    manifest::Writer writer("testds", false);
    writer.set("test", 1234, core::Interval());
    writer.flush();

    manifest::Reader reader("testds");
    reader.reread();
    const auto* seg = reader.segment("test");
    wassert_true(seg);
    wassert(actual(seg->relpath) == "test");
    wassert(actual(seg->mtime) == 1234);
    wassert(actual(seg->time) == core::Interval());
});

add_method("read_legacy_sqlite", [] {
    {
        manifest::Writer writer("testds", false);
        fill1(writer);
        create_sqlite(writer.file_list());
    }

    wassert(actual_file("testds/MANIFEST").not_exists());
    wassert(actual_file("testds/index.sqlite").exists());

    manifest::Reader reader("testds");
    reader.reread();

    const auto& segs = reader.file_list();
    wassert(actual(segs.size()) == 3u);
    wassert(actual(segs[0].relpath) == "2007/07-07.grib");
    wassert(actual(segs[1].relpath) == "2007/07-08.grib");
    wassert(actual(segs[2].relpath) == "2007/10-09.grib");
});

add_method("migrate_legacy_sqlite", [] {
    {
        manifest::Writer writer("testds", false);
        fill1(writer);
        create_sqlite(writer.file_list());
    }

    wassert(actual_file("testds/MANIFEST").not_exists());
    wassert(actual_file("testds/index.sqlite").exists());

    manifest::Writer writer("testds", false);
    wassert_false(writer.is_dirty());
    writer.reread();
    wassert_true(writer.is_dirty());
    writer.flush();
    wassert_false(writer.is_dirty());

    wassert(actual_file("testds/MANIFEST").exists());
    wassert(actual_file("testds/index.sqlite").not_exists());
});

}

}
