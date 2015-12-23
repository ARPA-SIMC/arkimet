#include "tests.h"
#include "segmented.h"
#include "maintenance.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/types/area.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include "arki/utils/sys.h"
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    Fixture(const std::string& extracfg)
    {
        std::stringstream in(extracfg);
        cfg.parse(in, "(memory)");
        cfg.setValue("path", sys::abspath("testds"));
        cfg.setValue("name", "testds");
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "product, area, reftime");
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override
    {
        add_method("compressed", [](Fixture& f) {
            // Test moving into archive data that have been compressed
            ConfigFile cfg = f.cfg;
            cfg.setValue("archive age", days_since(2007, 9, 1));

            // Import and compress all the files
            f.clean_and_import(&cfg);
            scan::compress("testds/2007/07-07.grib1");
            scan::compress("testds/2007/07-08.grib1");
            scan::compress("testds/2007/10-09.grib1");
            sys::unlink_ifexists("testds/2007/07-07.grib1");
            sys::unlink_ifexists("testds/2007/07-08.grib1");
            sys::unlink_ifexists("testds/2007/10-09.grib1");

            // Test that querying returns all items
            {
                std::unique_ptr<Reader> reader(f.makeReader(&cfg));
                unsigned count = count_results(*reader, Matcher());
                ensure_equals(count, 3u);
            }

            // Check if files to archive are detected
            {
                unique_ptr<SegmentedWriter> writer(f.makeLocalWriter(&cfg));

                MaintenanceResults expected(false, 3);
                expected.by_type[DatasetTest::COUNTED_OK] = 1;
                expected.by_type[DatasetTest::COUNTED_TO_ARCHIVE] = 2;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                unique_ptr<SegmentedWriter> writer(f.makeLocalWriter(&cfg));
                OutputChecker s;
                writer->repack(s, true);
                s.ensure_line_contains(": archived 2007/07-07.grib1");
                s.ensure_line_contains(": archived 2007/07-08.grib1");
                s.ensure_line_contains(": archive cleaned up");
                s.ensure_line_contains(": 2 files archived");
                s.ensure_all_lines_seen();
            }

            // Check that the files have been moved to the archive
            ensure(!sys::exists("testds/.archive/last/2007/07-07.grib1"));
            ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.gz"));
            ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.gz.idx"));
            ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.metadata"));
            ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.summary"));
            ensure(!sys::exists("testds/.archive/last/2007/07-08.grib1"));
            ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.gz"));
            ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.gz.idx"));
            ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.metadata"));
            ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.summary"));
            ensure(!sys::exists("testds/2007/07-07.grib1"));
            ensure(!sys::exists("testds/2007/07-07.grib1.gz"));
            ensure(!sys::exists("testds/2007/07-07.grib1.gz.idx"));
            ensure(!sys::exists("testds/2007/07-07.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-07.grib1.summary"));
            ensure(!sys::exists("testds/2007/07-08.grib1"));
            ensure(!sys::exists("testds/2007/07-08.grib1.gz"));
            ensure(!sys::exists("testds/2007/07-08.grib1.gz.idx"));
            ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));

            // Maintenance should now show a normal situation
            {
                unique_ptr<SegmentedWriter> writer(f.makeLocalWriter(&cfg));
                MaintenanceResults expected(true, 3);
                expected.by_type[DatasetTest::COUNTED_OK] = 1;
                expected.by_type[DatasetTest::COUNTED_ARC_OK] = 2;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform full maintenance and check that things are still ok afterwards
            {
                unique_ptr<SegmentedWriter> writer(f.makeLocalWriter(&cfg));
                stringstream s;
                s.str(std::string());
                writer->check(s, true, true);
                ensure_equals(s.str(), string()); // Nothing should have happened

                MaintenanceResults expected(true, 3);
                expected.by_type[DatasetTest::COUNTED_OK] = 1;
                expected.by_type[DatasetTest::COUNTED_ARC_OK] = 2;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Test that querying returns all items
            {
                std::unique_ptr<Reader> reader(f.makeReader(&cfg));
                ensure_equals(count_results(*reader, Matcher()), 3u);
            }
        });
        add_method("query", [](Fixture& f) {
            // Test querying the datasets
            using namespace arki::types;

            f.clean_and_import();
            std::unique_ptr<Reader> reader(f.makeReader());

            metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,200"));
            ensure_equals(mdc.size(), 1u);

            // Check that the source record that comes out is ok
            wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::abspath("testds"), "2007/07-08.grib1", 0, 7218));

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,80"));
            ensure_equals(mdc.size(), 1u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,98"));
            ensure_equals(mdc.size(), 1u);
        });
        add_method("query_data", [](Fixture& f) {
            // Test querying with data only
            f.clean_and_import();
            std::unique_ptr<Reader> reader(f.makeReader());

            sys::File out(sys::File::mkstemp("test"));
            dataset::ByteQuery bq;
            bq.setData(Matcher::parse("origin:GRIB1,200"));
            reader->query_bytes(bq, out);

            string res = sys::read_file(out.name());
            ensure_equals(res.substr(0, 4), "GRIB");
        });
        add_method("query_inline", [](Fixture& f) {
            // Test querying with inline data
            using namespace arki::types;

            f.clean_and_import();
            std::unique_ptr<Reader> reader(f.makeReader());

            metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,200"));
            ensure_equals(mdc.size(), 1u);

            // Check that the source record that comes out is ok
            //wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));

            // Check that data is accessible
            const auto& buf = mdc[0].getData();
            ensure_equals(buf.size(), 7218u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,80"));
            ensure_equals(mdc.size(), 1u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,98"));
            ensure_equals(mdc.size(), 1u);
        });
        add_method("query_archived", [](Fixture& f) {
            // Test querying with archived data
            using namespace arki::types;

            ConfigFile cfg = f.cfg;
            cfg.setValue("archive age", days_since(2007, 9, 1));
            f.clean_and_import(&cfg);
            {
                unique_ptr<LocalWriter> writer(f.makeLocalWriter(&cfg));
                OutputChecker s;
                writer->repack(s, true);
                s.ensure_line_contains(": archived 2007/07-07.grib1");
                s.ensure_line_contains(": archived 2007/07-08.grib1");
                s.ignore_line_containing("archive cleaned up");
                s.ensure_line_contains(": 2 files archived");
                s.ensure_all_lines_seen();
            }

            std::unique_ptr<Reader> reader(f.makeReader(&cfg));
            metadata::Collection mdc(*reader, Matcher::parse(""));
            ensure_equals(mdc.size(), 3u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,200"));
            ensure_equals(mdc.size(), 1u);

            // Check that the source record that comes out is ok
            //wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));

            // Check that data is accessible
            const auto& buf = mdc[0].getData();
            wassert(actual(buf.size()) == 7218u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("reftime:=2007-07-08"));
            ensure_equals(mdc.size(), 1u);
            wassert(actual(mdc[0].data_size()) == 7218u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,80"));
            ensure_equals(mdc.size(), 1u);

            mdc.clear();
            mdc.add(*reader, Matcher::parse("origin:GRIB1,98"));
            ensure_equals(mdc.size(), 1u);

            // Query bytes
            sys::File out(sys::File::mkstemp("test"));
            dataset::ByteQuery bq;
            bq.setData(Matcher());
            reader->query_bytes(bq, out);
            string res = sys::read_file(out.name());
            ensure_equals(res.size(), 44412u);

            out.lseek(0); out.ftruncate(0);
            bq.matcher = Matcher::parse("origin:GRIB1,200");
            reader->query_bytes(bq, out);
            res = sys::read_file(out.name());
            ensure_equals(res.size(), 7218u);

            out.lseek(0); out.ftruncate(0);
            bq.matcher = Matcher::parse("reftime:=2007-07-08");
            reader->query_bytes(bq, out);
            res = sys::read_file(out.name());
            ensure_equals(res.size(), 7218u);

            /* TODO
                case BQ_POSTPROCESS:
                case BQ_REP_METADATA:
                case BQ_REP_SUMMARY:
            virtual void querySummary(const Matcher& matcher, Summary& summary);
            */

            // Query summary
            Summary s;
            reader->querySummary(Matcher::parse(""), s);
            ensure_equals(s.count(), 3u);
            ensure_equals(s.size(), 44412u);

            s.clear();
            reader->querySummary(Matcher::parse("origin:GRIB1,200"), s);
            ensure_equals(s.count(), 1u);
            ensure_equals(s.size(), 7218u);

            s.clear();
            reader->querySummary(Matcher::parse("reftime:=2007-07-08"), s);
            ensure_equals(s.count(), 1u);
            ensure_equals(s.size(), 7218u);
        });
        add_method("empty_dirs", [](Fixture& f) {
            // Tolerate empty dirs
            // Start with an empty dir
            system("rm -rf testds");
            system("mkdir testds");

            std::unique_ptr<Reader> reader(f.makeReader());

            metadata::Collection mdc(*reader, Matcher());
            ensure(mdc.empty());

            Summary s;
            reader->querySummary(Matcher::parse(""), s);
            ensure_equals(s.count(), 0u);

            sys::File out(sys::File::mkstemp("test"));
            dataset::ByteQuery bq;
            bq.setData(Matcher::parse(""));
            reader->query_bytes(bq, out);
            out.close();
            wassert(actual(sys::size(out.name())) == 0u);
        });
        add_method("query_lots", [](Fixture& f) {
            // Test querying with lots of data, to trigger on disk metadata buffering
            using namespace arki::types;

            f.clean();
            {
                std::unique_ptr<Writer> writer(f.makeWriter());

                // Reverse order of import, so we can check if things get sorted properly when querying
                for (int month = 12; month >= 1; --month)
                    for (int day = 28; day >= 1; --day)
                        for (int hour = 21; hour >= 0; hour -= 3)
                            for (int station = 2; station >= 1; --station)
                                for (int varid = 3; varid >= 1; --varid)
                                {
                                    int value = month + day + hour + station + varid;
                                    Metadata md;
                                    char buf[40];
                                    int len = snprintf(buf, 40, "2013%02d%02d%02d00,%d,%d,%d,,,000000000",
                                            month, day, hour, station, varid, value);
                                    md.set_source_inline("vm2", vector<uint8_t>(buf, buf+len));
                                    md.add_note("Generated from memory");
                                    md.set(Reftime::createPosition(Time(2013, month, day, hour, 0, 0)));
                                    md.set(Area::createVM2(station));
                                    md.set(Product::createVM2(varid));
                                    snprintf(buf, 40, "%d,,,000000000", value);
                                    md.set(types::Value::create(buf));
                                    Writer::AcquireResult res = writer->acquire(md);
                                    wassert(actual(res) == Writer::ACQ_OK);
                                }
            }

            utils::files::removeDontpackFlagfile(f.cfg.value("path"));

            // Query all the dataset and make sure the results are in the right order
            struct CheckSortOrder : public metadata::Eater
            {
                uint64_t last_value;
                unsigned seen;
                CheckSortOrder() : last_value(0), seen(0) {}
                virtual uint64_t make_key(const Metadata& md) const = 0;
                bool eat(unique_ptr<Metadata>&& md) override
                {
                    uint64_t value = make_key(*md);
                    wassert(actual(value) >= last_value);
                    last_value = value;
                    ++seen;
                    return true;
                }
            };

            struct CheckReftimeSortOrder : public CheckSortOrder
            {
                virtual uint64_t make_key(const Metadata& md) const
                {
                    const reftime::Position* rt = md.get<types::reftime::Position>();
                    return rt->time.vals[1] * 10000 + rt->time.vals[2] * 100 + rt->time.vals[3];
                }
            };

            struct CheckAllSortOrder : public CheckSortOrder
            {
                virtual uint64_t make_key(const Metadata& md) const
                {
                    const reftime::Position* rt = md.get<types::reftime::Position>();
                    const area::VM2* area = dynamic_cast<const area::VM2*>(md.get(TYPE_AREA));
                    const product::VM2* prod = dynamic_cast<const product::VM2*>(md.get(TYPE_PRODUCT));
                    uint64_t dt = rt->time.vals[1] * 10000 + rt->time.vals[2] * 100 + rt->time.vals[3];
                    return dt * 100 + area->station_id() * 10 + prod->variable_id();
                }
            };

            {
                std::unique_ptr<Reader> reader(f.makeReader());
                CheckReftimeSortOrder cso;
                reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
                wassert(actual(cso.seen) == 16128u);
            }

            {
                std::unique_ptr<Reader> reader(f.makeReader());
                CheckAllSortOrder cso;
                dataset::DataQuery dq(Matcher::parse(""));
                dq.sorter = sort::Compare::parse("reftime,area,product");
                reader->query_data(dq, [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
                wassert(actual(cso.seen) == 16128u);
            }
        });
    }
};

Tests test_ondisk2("arki_dataset_segmented_ondisk2", "type=ondisk2\n");
Tests test_simple_plain("arki_dataset_segmented_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_segmented_simple_sqlite", "type=simple\nindex_type=sqlite");

}

