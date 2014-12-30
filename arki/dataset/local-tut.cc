/*
 * Copyright (C) 2007--2013  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include "config.h"

#include <arki/dataset/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/dataset/local.h>
#include <arki/dataset/maintenance.h>
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <arki/types/area.h>
#include <arki/types/product.h>
#include <arki/types/value.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace wibble::tests;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;
using namespace wibble;

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
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));

	// Import and compress all the files
	clean_and_import(&cfg);
	scan::compress("testds/2007/07-07.grib1");
	scan::compress("testds/2007/07-08.grib1");
	scan::compress("testds/2007/10-09.grib1");
	sys::fs::deleteIfExists("testds/2007/07-07.grib1");
	sys::fs::deleteIfExists("testds/2007/07-08.grib1");
	sys::fs::deleteIfExists("testds/2007/10-09.grib1");

	// Test that querying returns all items
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

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
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ensure_line_contains(": archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	// Check that the files have been moved to the archive
	ensure(!sys::fs::exists("testds/.archive/last/2007/07-07.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.gz"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.gz.idx"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.summary"));
	ensure(!sys::fs::exists("testds/.archive/last/2007/07-08.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.gz"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.gz.idx"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.summary"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.gz"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.gz.idx"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.metadata"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.summary"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.gz"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.gz.idx"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.metadata"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.summary"));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
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
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}
}

// Test querying the datasets
template<> template<> void to::test<2>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    UItem<Source> source = mdc[0].source;
    wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testds"), "2007/07-08.grib1", 0, 7218));

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with data only
template<> template<> void to::test<3>()
{
	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse("origin:GRIB1,200"));
	reader->queryBytes(bq, os);

	ensure_equals(os.str().substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<> void to::test<4>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::INLINE);
	ensure_equals(source->format, "grib1");
    wassert(actual(source->getSize()) == 7218u);

    wibble::sys::Buffer buf = mdc[0].getData();
    ensure_equals(buf.size(), source->getSize());

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with archived data
template<> template<> void to::test<5>()
{
	using namespace arki::types;

	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));
	clean_and_import(&cfg);
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ignore_line_containing("archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));
	metadata::Collection mdc;

	reader->queryData(dataset::DataQuery(Matcher::parse(""), true), mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.clear();

	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::INLINE);
	ensure_equals(source->format, "grib1");
    wassert(actual(source->getSize()) == 7218u);

    wibble::sys::Buffer buf = mdc[0].getData();
    wassert(actual(buf.size()) == source->getSize());

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("reftime:=2007-07-08"), true), mdc);
	ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source->getSize()) == 7218u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Query bytes
	stringstream out;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 44412u);

	out.str(string());
	bq.matcher = Matcher::parse("origin:GRIB1,200");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	out.str(string());
	bq.matcher = Matcher::parse("reftime:=2007-07-08");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

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

	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure(mdc.empty());

	Summary s;
	reader->querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 0u);

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, os);
	ensure(os.str().empty());
}

// Test querying with lots of data, to trigger on disk metadata buffering
template<> template<> void to::test<7>()
{
    using namespace arki::types;

    clean();
    {
        std::auto_ptr<WritableDataset> writer(makeWriter());

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
                            wibble::sys::Buffer data(buf, len, false);
                            md.source = types::Source::createInline("vm2", data);
                            md.add_note(types::Note::create("Generated from memory"));
                            md.set(types::reftime::Position::create(types::Time::create(2013, month, day, hour, 0, 0)));
                            md.set(types::area::VM2::create(station));
                            md.set(types::product::VM2::create(varid));
                            snprintf(buf, 40, "%d,,,000000000", value);
                            md.set(types::Value::create(buf));
                            WritableDataset::AcquireResult res = writer->acquire(md);
                            wassert(actual(res) == WritableDataset::ACQ_OK);
                        }
    }

    utils::files::removeDontpackFlagfile(cfg.value("path"));

    // Query all the dataset and make sure the results are in the right order
    struct CheckSortOrder : public metadata::Consumer
    {
        uint64_t last_value;
        unsigned seen;
        CheckSortOrder() : last_value(0), seen(0) {}
        virtual uint64_t make_key(const Metadata& md) const = 0;
        virtual bool operator()(Metadata& md)
        {
            uint64_t value = make_key(md);
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
            UItem<types::reftime::Position> rt = md.get<types::reftime::Position>();
            return rt->time->vals[1] * 10000 + rt->time->vals[2] * 100 + rt->time->vals[3];
        }
    };

    struct CheckAllSortOrder : public CheckSortOrder
    {
        virtual uint64_t make_key(const Metadata& md) const
        {
            UItem<types::reftime::Position> rt = md.get<types::reftime::Position>();
            UItem<types::area::VM2> area = md.get(TYPE_AREA).upcast<types::area::VM2>();
            UItem<types::product::VM2> prod = md.get(TYPE_PRODUCT).upcast<types::product::VM2>();
            uint64_t dt = rt->time->vals[1] * 10000 + rt->time->vals[2] * 100 + rt->time->vals[3];
            return dt * 100 + area->station_id() * 10 + prod->variable_id();
        }
    };

    {
        std::auto_ptr<ReadonlyDataset> reader(makeReader());
        CheckReftimeSortOrder cso;
        reader->queryData(dataset::DataQuery(Matcher::parse(""), false), cso);
        wassert(actual(cso.seen) == 16128);
    }

    {
        std::auto_ptr<ReadonlyDataset> reader(makeReader());
        CheckAllSortOrder cso;
        dataset::DataQuery dq(Matcher::parse(""), false);
        dq.sorter = sort::Compare::parse("reftime,area,product");
        reader->queryData(dq, cso);
        wassert(actual(cso.seen) == 16128);
    }
}

}

namespace {

tut::tg test_ondisk2("arki_dataset_local_ondisk2", "type=ondisk2\n");
tut::tg test_simple_plain("arki_dataset_local_simple_plain", "type=simple\nindex_type=plain\n");
tut::tg test_simple_sqlite("arki_dataset_local_simple_sqlite", "type=simple\nindex_type=sqlite");

}
