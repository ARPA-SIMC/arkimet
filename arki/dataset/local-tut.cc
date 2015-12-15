#include "config.h"
#include "arki/dataset/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/dataset/local.h"
#include "arki/dataset/maintenance.h"
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
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::types;

struct arki_dataset_local_base : public DatasetTest {
    arki_dataset_local_base()
    {
        cfg.setValue("path", "testds");
        cfg.setValue("name", "testds");
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "product, area, reftime");
    }
};

}

namespace tut {

typedef dataset_tg<arki_dataset_local_base> tg;
typedef tg::object to;

// Test moving into archive data that have been compressed
template<> template<> void to::test<1>()
{
    ConfigFile cfg = this->cfg;
    cfg.setValue("archive age", days_since(2007, 9, 1));

    // Import and compress all the files
    clean_and_import(&cfg);
    scan::compress("testds/2007/07-07.grib1");
    scan::compress("testds/2007/07-08.grib1");
    scan::compress("testds/2007/10-09.grib1");
    sys::unlink_ifexists("testds/2007/07-07.grib1");
    sys::unlink_ifexists("testds/2007/07-08.grib1");
    sys::unlink_ifexists("testds/2007/10-09.grib1");

    // Test that querying returns all items
    {
        std::unique_ptr<ReadonlyDataset> reader(makeReader(&cfg));
        unsigned count = count_results(*reader, Matcher());
        ensure_equals(count, 3u);
    }

    // Check if files to archive are detected
    {
        unique_ptr<WritableSegmented> writer(makeLocalWriter(&cfg));

        MaintenanceCollector c;
        writer->maintenance(c);

        ensure_equals(c.fileStates.size(), 3u);
        ensure_equals(c.count(COUNTED_OK), 1u);
        ensure_equals(c.count(COUNTED_TO_ARCHIVE), 2u);
        ensure_equals(c.remaining(), "");
        ensure(not c.isClean());
    }

    // Perform packing and check that things are still ok afterwards
    {
        unique_ptr<WritableSegmented> writer(makeLocalWriter(&cfg));
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
        unique_ptr<WritableSegmented> writer(makeLocalWriter(&cfg));
        MaintenanceCollector c;
        writer->maintenance(c);
        ensure_equals(c.count(COUNTED_OK), 1u);
        ensure_equals(c.count(COUNTED_ARC_OK), 2u);
        ensure_equals(c.remaining(), "");
        ensure(c.isClean());
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        unique_ptr<WritableSegmented> writer(makeLocalWriter(&cfg));
        stringstream s;
        s.str(std::string());
        writer->check(s, true, true);
        ensure_equals(s.str(), string()); // Nothing should have happened

		MaintenanceCollector c;
		c.clear();
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

    // Test that querying returns all items
    {
        std::unique_ptr<ReadonlyDataset> reader(makeReader(&cfg));
        ensure_equals(count_results(*reader, Matcher()), 3u);
    }
}

// Test querying the datasets
template<> template<> void to::test<2>()
{
	using namespace arki::types;

    clean_and_import();
    std::unique_ptr<ReadonlyDataset> reader(makeReader());

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
}

// Test querying with data only
template<> template<> void to::test<3>()
{
	clean_and_import();
	std::unique_ptr<ReadonlyDataset> reader(makeReader());

    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(Matcher::parse("origin:GRIB1,200"));
    reader->query_bytes(bq, out);

    string res = sys::read_file(out.name());
    ensure_equals(res.substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<> void to::test<4>()
{
	using namespace arki::types;

    clean_and_import();
    std::unique_ptr<ReadonlyDataset> reader(makeReader());

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
}

// Test querying with archived data
template<> template<> void to::test<5>()
{
	using namespace arki::types;

    ConfigFile cfg = this->cfg;
    cfg.setValue("archive age", days_since(2007, 9, 1));
    clean_and_import(&cfg);
	{
		unique_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ignore_line_containing("archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

    std::unique_ptr<ReadonlyDataset> reader(makeReader(&cfg));
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
}

// Tolerate empty dirs
template<> template<> void to::test<6>()
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	std::unique_ptr<ReadonlyDataset> reader(makeReader());

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
}

// Test querying with lots of data, to trigger on disk metadata buffering
template<> template<> void to::test<7>()
{
    using namespace arki::types;

    clean();
    {
        std::unique_ptr<WritableDataset> writer(makeWriter());

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
                            WritableDataset::AcquireResult res = writer->acquire(md);
                            wassert(actual(res) == WritableDataset::ACQ_OK);
                        }
    }

    utils::files::removeDontpackFlagfile(cfg.value("path"));

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
        std::unique_ptr<ReadonlyDataset> reader(makeReader());
        CheckReftimeSortOrder cso;
        reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }

    {
        std::unique_ptr<ReadonlyDataset> reader(makeReader());
        CheckAllSortOrder cso;
        dataset::DataQuery dq(Matcher::parse(""));
        dq.sorter = sort::Compare::parse("reftime,area,product");
        reader->query_data(dq, [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }
}

}

namespace {

tut::tg test_ondisk2("arki_dataset_local_ondisk2", "type=ondisk2\n");
tut::tg test_simple_plain("arki_dataset_local_simple_plain", "type=simple\nindex_type=plain\n");
tut::tg test_simple_sqlite("arki_dataset_local_simple_sqlite", "type=simple\nindex_type=sqlite");

}
