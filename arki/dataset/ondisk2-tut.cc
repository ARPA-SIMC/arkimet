/*
 * Copyright (C) 2007--2015  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/tests.h>
#include <arki/dataset/ondisk2.h>
//#include <arki/dataset/ondisk2/maintenance.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/source/blob.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/time.h>
#include <arki/scan/grib.h>
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/childprocess.h>
#include <unistd.h>

#include <sstream>
#include <fstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk2;
using namespace arki::utils;
using namespace arki::tests;

namespace {
static inline const types::AssignedDataset* getDataset(const Metadata& md)
{
    return md.get<AssignedDataset>();
}
}

struct arki_dataset_ondisk2_shar {
	ConfigFile config;
	ConfigFile configAll;
    testdata::GRIBData tdata_grib;
    testdata::BUFRData tdata_bufr;
    testdata::VM2Data tdata_vm2;
    testdata::ODIMData tdata_odim;

	arki_dataset_ondisk2_shar()
	{
		// Cleanup the test datasets
		system("rm -rf testall/*");
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf test98/*");

		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[test98]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,98\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test98\n"
			"path = test98\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");


		// In-memory dataset configuration
		string conf1 =
			"[testall]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1\n"
			"index = origin, reftime\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = testall\n"
			"path = testall\n";
		stringstream incfg1(conf1);
		configAll.parse(incfg1, "(memory)");
	}

	void acquireSamples()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf test98/*");

		// Import data into the datasets
		Metadata md;
		scan::Grib scanner;
		dataset::ondisk2::Writer d200(*config.section("test200"));
		dataset::ondisk2::Writer d80(*config.section("test80"));
		dataset::ondisk2::Writer d98(*config.section("test98"));
		scanner.open("inbound/test.grib1");
		ensure(scanner.next(md));
		ensure_equals(d200.acquire(md), WritableDataset::ACQ_OK);
		ensure(scanner.next(md));
		ensure_equals(d80.acquire(md), WritableDataset::ACQ_OK);
		ensure(scanner.next(md));
		ensure_equals(d98.acquire(md), WritableDataset::ACQ_OK);
		ensure(!scanner.next(md));

		// Finalise
		d200.flush();
		d80.flush();
		d98.flush();
	}

	void acquireSamplesAllInOne()
	{
		// Cleanup the test datasets
		system("rm -rf testall/*");

		// Import data into the datasets
		Metadata md;
		scan::Grib scanner;
		dataset::ondisk2::Writer all(*configAll.section("testall"));
		scanner.open("inbound/test.grib1");
		ensure(scanner.next(md));
		ensure_equals(all.acquire(md), WritableDataset::ACQ_OK);
		ensure(scanner.next(md));
		ensure_equals(all.acquire(md), WritableDataset::ACQ_OK);
		ensure(scanner.next(md));
		ensure_equals(all.acquire(md), WritableDataset::ACQ_OK);
		ensure(!scanner.next(md));

		// Finalise
		all.flush();
	}
};
TESTGRP(arki_dataset_ondisk2);

// Work-around for a bug in old gcc versions
#if __GNUC__ && __GNUC__ < 4
static string emptystring;
#define string() emptystring
#endif

// Test acquiring data
template<> template<>
void to::test<1>()
{
	// Clean the dataset
	system("rm -rf test200/*");

	metadata::Collection mdc;
	wassert(actual(scan::scan("inbound/test.grib1", mdc)).istrue());

	dataset::ondisk2::Writer d200(*config.section("test200"));

	// Import once in the empty dataset
	WritableDataset::AcquireResult res = d200.acquire(mdc[0]);
	ensure_equals(res, WritableDataset::ACQ_OK);
	#if 0
	for (vector<Note>::const_iterator i = md.notes.begin();
			i != md.notes.end(); ++i)
		cerr << *i << endl;
	#endif
    const AssignedDataset* ds = getDataset(mdc[0]);
    ensure_equals(ds->name, "test200");
    ensure_equals(ds->id, "1");

    // See if we catch duplicate imports
    mdc[0].unset(TYPE_ASSIGNEDDATASET);
    res = d200.acquire(mdc[0]);
    ensure_equals(res, WritableDataset::ACQ_ERROR_DUPLICATE);
    ds = getDataset(mdc[0]);
    ensure(!ds);

	// Flush the changes and check that everything is allright
	d200.flush();

	ensure(sys::fs::exists("test200/2007/07-08.grib1"));
	ensure(sys::fs::exists("test200/index.sqlite"));
    ensure(sys::fs::timestamp("test200/2007/07-08.grib1") <= sys::fs::timestamp("test200/index.sqlite"));
//2	ensure(!hasRebuildFlagfile("test200/2007/07-08.grib1"));
//2	ensure(!hasIndexFlagfile("test200"));
}

// Test querying the datasets
template<> template<>
void to::test<2>()
{
	acquireSamples();
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
	metadata::Collection mdc;

    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::fs::abspath("test200"), "2007/07-08.grib1", 0, 7218));

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98")), mdc);
    ensure_equals(mdc.size(), 0u);
}

// Test querying the datasets
template<> template<>
void to::test<3>()
{
    acquireSamples();
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test80")));
    metadata::Collection mdc;
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::fs::abspath("test80"), "2007/07-07.grib1", 0, 34960));

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98")), mdc);
    ensure_equals(mdc.size(), 0u);
}

// Test querying the datasets
template<> template<>
void to::test<4>()
{
    acquireSamples();
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test98")));
    metadata::Collection mdc;
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98")), mdc);
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::fs::abspath("test98"), "2007/10-09.grib1", 0, 2234));
}

// Test replacing an element
template<> template<>
void to::test<5>()
{
	acquireSamples();

	metadata::Collection mdc;
	{
		auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test80")));

        // Fetch an element
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
        ensure_equals(mdc.size(), 1u);
    }

    // Take note of the original source
    auto_ptr<source::Blob> blob1(mdc[0].sourceBlob().clone());

	{
		auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("test80")));
		// Replace it
		if (testds->acquire(mdc[0], WritableDataset::REPLACE_ALWAYS) != WritableDataset::ACQ_OK)
		{
            std::vector<Note> notes = mdc[0].notes();
			for (size_t i = 0; i < notes.size(); ++i)
				cerr << " md note: " << notes[i] << endl;
			ensure(false);
		}
		testds->flush();
	}

    {
        auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test80")));

        // Fetch the element again
        mdc.clear();
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
        ensure_equals(mdc.size(), 1u);
    }

    // Take note of the original source
    auto_ptr<source::Blob> blob2(mdc[0].sourceBlob().clone());

	// Ensure that it's on the same file (since the reftime did not change)
	ensure_equals(blob1->filename, blob2->filename);

	// Ensure that it's on a different position
	ensure(blob1->offset < blob2->offset);

	// Ensure that it's the same length
	ensure_equals(blob1->size, blob2->size);
}

// Test removing an element
template<> template<>
void to::test<6>()
{
	acquireSamples();
	metadata::Collection mdc;
	{
		auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

        // Fetch an element
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
        ensure_equals(mdc.size(), 1u);
    }

	// Check that it has a source and metadata element
    const AssignedDataset* ds = getDataset(mdc[0]);
    Time changeTime = ds->changed;
    ensure_equals(ds->name, "test200");
    wassert(actual(mdc[0].has_source_blob()).istrue());

	{
		auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("test200")));
		ensure(!sys::fs::exists("test200/2007/07-08.grib1.needs-pack"));
		// Remove it
		testds->remove(mdc[0]);
		testds->flush();
		ensure(!sys::fs::exists("test200/2007/07-08.grib1.needs-pack"));
	}

    // Check that it does not have a source and metadata element
    ds = getDataset(mdc[0]);
    ensure(!ds);
    wassert(actual(mdc[0].has_source()).isfalse());

    // Try to fetch the element again
    {
        auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
        mdc.clear();
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
        ensure_equals(mdc.size(), 0u);
    }

#if 0
	// Try to fetch all other elements
	{
		auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
		mdc.clear();
		testds->queryData(dataset::DataQuery(Matcher::parse("")), mdc);
		ensure_equals(mdc.size(), 2u);
	}

	// TODO: Repack

	// Try to fetch the element again
	{
		auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
		mdc.clear();
		testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 0u);
	}

	// Try to fetch all other elements
	{
		auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
		mdc.clear();
		testds->queryData(dataset::DataQuery(Matcher::parse("")), mdc);
		ensure_equals(mdc.size(), 2u);
	}
#endif
}

// Test reindexing
template<> template<>
void to::test<7>()
{
	// TODO
#if 0
	acquireSamples();

	// Try a query
	{
		Reader reader(*config.section("test200"));
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 1u);
	}

	// Rebuild the index
	{
		dataset::ondisk2::Writer d200(*config.section("test200"));
		// Run a full maintenance run
		d200.invalidateAll();
		metadata::Collection mdc;
		stringstream log;
		FullMaintenance fr(log, mdc);
		//FullMaintenance fr(cerr, mdc);
		d200.maintenance(fr);
	}

	// See that the query still works
	{
		Reader reader(*config.section("test200"));
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 1u);
	}
#endif
}

// Test querying the summary
template<> template<>
void to::test<8>()
{
	acquireSamples();
	Reader reader(*config.section("test200"));
	Summary summary;
	reader.querySummary(Matcher::parse("origin:GRIB1,200"), summary);
	ensure_equals(summary.count(), 1u);
}

// Test querying the summary by reftime
template<> template<>
void to::test<9>()
{
	acquireSamples();
	Reader reader(*config.section("test200"));
	Summary summary;
	//system("bash");
	reader.querySummary(Matcher::parse("reftime:>=2007-07"), summary);
	ensure_equals(summary.count(), 1u);
}

// Test acquiring data with replace=1
template<> template<>
void to::test<10>()
{
	// Clean the dataset
	system("rm -rf test200/*");

	Metadata md;

	// Import once
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer d200(*config.section("test200"));
		size_t count;
		for (count = 0; scanner.next(md); ++count)
			ensure(d200.acquire(md) == WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		d200.flush();
	}

	// Import again, make sure they're all duplicates
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer d200(*config.section("test200"));
		size_t count;
		for (count = 0; scanner.next(md); ++count)
			ensure(d200.acquire(md) == WritableDataset::ACQ_ERROR_DUPLICATE);
		ensure_equals(count, 3u);
		d200.flush();
	}

	// Import again with replace=true, make sure they're all ok
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		ConfigFile cfg = *config.section("test200");
		cfg.setValue("replace", "true");
		dataset::ondisk2::Writer d200(cfg);
		size_t count;
		for (count = 0; scanner.next(md); ++count)
			ensure(d200.acquire(md) == WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		d200.flush();
	}

	// Make sure all the data files need repack, as they now have got deleted data inside
	//ensure(hasPackFlagfile("test200/2007/07-08.grib1"));
	//ensure(hasPackFlagfile("test200/2007/07-07.grib1"));
	//ensure(hasPackFlagfile("test200/2007/10-09.grib1"));

    // Test querying the dataset
    {
        Reader reader(*config.section("test200"));
        metadata::Collection mdc;
        reader.queryData(dataset::DataQuery(Matcher()), mdc);
        ensure_equals(mdc.size(), 3u);

        // Make sure we're not getting the deleted element
        const source::Blob& blob0 = mdc[0].sourceBlob();
        ensure(blob0.offset > 0);
        const source::Blob& blob1 = mdc[1].sourceBlob();
        ensure(blob1.offset > 0);
        const source::Blob& blob2 = mdc[2].sourceBlob();
        ensure(blob2.offset > 0);
    }

	// Test querying the summary
	{
		Reader reader(*config.section("test200"));
		Summary summary;
		reader.querySummary(Matcher(), summary);
		ensure_equals(summary.count(), 3u);
	}
}

// Test querying the first reftime extreme of the summary
template<> template<>
void to::test<11>()
{
	acquireSamplesAllInOne();
	Reader reader(*configAll.section("testall"));
	Summary summary;
	reader.querySummary(Matcher(), summary);
	ensure_equals(summary.count(), 3u);

    auto_ptr<Reftime> rt = summary.getReferenceTime();
    ensure_equals(rt->style(), Reftime::PERIOD);
    auto_ptr<reftime::Period> p = downcast<reftime::Period>(rt);
    metadata::Collection mdc;
    reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80; reftime:=" + p->begin.toISO8601())), mdc);
    ensure_equals(mdc.size(), 1u);

    mdc.clear();
    reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98; reftime:=" + p->end.toISO8601())), mdc);
    ensure_equals(mdc.size(), 1u);
}

// Test querying data using index
template<> template<>
void to::test<12>()
{
	acquireSamplesAllInOne();
	Reader reader(*configAll.section("testall"));
	stringstream str;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse("reftime:=2007"));
	reader.queryBytes(bq, str);
	ensure_equals(str.str().size(), 44412u);
}

namespace {
struct ReadHang : public sys::ChildProcess, public metadata::Eater
{
	const ConfigFile& cfg;
	int commfd;

	ReadHang(const ConfigFile& cfg) : cfg(cfg) {}

    bool eat(auto_ptr<Metadata> md) override
	{
		// Notify start of reading
		cout << "H" << endl;
		// Get stuck while reading
		while (true)
			usleep(100000);
		return true;
	}

    virtual int main()
    {
        try {
            Reader reader(cfg);
            reader.queryData(dataset::DataQuery(Matcher()), *this);
        } catch (std::exception& e) {
			cerr << e.what() << endl;
			cout << "E" << endl;
			return 1;
		}
		return 0;
	}

	void start()
	{
		forkAndRedirect(0, &commfd);
	}

	char waitUntilHung()
	{
		char buf[2];
		if (read(commfd, buf, 1) != 1)
			throw wibble::exception::System("reading 1 byte from child process");
		return buf[0];
	}
};
}

// Test acquiring with a reader who's stuck on output
template<> template<>
void to::test<13>()
{
	// Clean the dataset
	system("rm -rf test200/*");

	Metadata md;

	// Import one grib in the dataset
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer d200(*config.section("test200"));
		scanner.next(md);
		ensure(d200.acquire(md) == WritableDataset::ACQ_OK);
		d200.flush();
	}

	// Query the index and hang
	ReadHang readHang(*config.section("test200"));
	readHang.start();
	ensure_equals(readHang.waitUntilHung(), 'H');

	// Import another grib in the dataset
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer d200(*config.section("test200"));
		scanner.next(md);
		scanner.next(md);
		ensure(d200.acquire(md) == WritableDataset::ACQ_OK);
		d200.flush();
	}

	readHang.kill(9);
	readHang.wait();
}

// Test acquiring data on a compressed file
template<> template<>
void to::test<14>()
{
	// In-memory dataset configuration
	string conf =
		"[testall]\n"
		"type = ondisk2\n"
		"step = yearly\n"
		"filter = origin: GRIB1\n"
		"index = origin, reftime\n"
		"unique = reftime, origin, product, level, timerange, area\n"
		"name = testall\n"
		"path = testall\n";
	stringstream incfg(conf);
	config.parse(incfg, "(memory)");

	// Clean the dataset
	system("rm -rf testall/*");

	Metadata md;
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");

	// Import the first two
	{
		dataset::ondisk2::Writer all(*config.section("testall"));
		ensure(scanner.next(md));
		ensure_equals(all.acquire(md), WritableDataset::ACQ_OK);
	}
	ensure(sys::fs::exists("testall/20/2007.grib1"));

    // Compress what is imported so far
    {
        metadata::Collection mdc;
        Reader reader(*config.section("testall"));
        reader.queryData(dataset::DataQuery(Matcher::parse("")), mdc);
        ensure_equals(mdc.size(), 1u);
        mdc.compressDataFile(1024, "metadata file testall/20/2007.grib1");
        sys::fs::deleteIfExists("testall/20/2007.grib1");
    }
    ensure(!sys::fs::exists("testall/20/2007.grib1"));

	// Import the last data
	{
		dataset::ondisk2::Writer all(*config.section("testall"));
		ensure(scanner.next(md));
		try {
			ensure_equals(all.acquire(md), WritableDataset::ACQ_OK);
		} catch (std::exception& e) {
			ensure(string(e.what()).find("cannot update compressed data files") != string::npos);
		}
	}
	ensure(!sys::fs::exists("testall/20/2007.grib1"));
}

// Test Update Sequence Number replacement strategy
template<> template<>
void to::test<15>()
{
    // Configure a BUFR dataset
    system("rm -rf testbufr/*");
    string conf =
        "[testbufr]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "filter = origin:BUFR\n"
        "unique = reftime\n"
        "name = testbufr\n"
        "path = testbufr\n";
    stringstream incfg(conf);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");

    dataset::ondisk2::Writer bd(*cfg.section("testbufr"));

    metadata::Collection mdc;
    ensure(scan::scan("inbound/synop-gts.bufr", mdc));

    metadata::Collection mdc_upd;
    ensure(scan::scan("inbound/synop-gts-usn2.bufr", mdc_upd));

    // Acquire
    ensure_equals(bd.acquire(mdc[0]), WritableDataset::ACQ_OK);

    // Acquire again: it fails
    ensure_equals(bd.acquire(mdc[0]), WritableDataset::ACQ_ERROR_DUPLICATE);

    // Acquire again: it fails even with a higher USN
    ensure_equals(bd.acquire(mdc_upd[0]), WritableDataset::ACQ_ERROR_DUPLICATE);

    // Acquire with replace: it works
    ensure_equals(bd.acquire(mdc[0], WritableDataset::REPLACE_ALWAYS), WritableDataset::ACQ_OK);

    // Acquire with USN: it works, since USNs the same as the existing ones do overwrite
    ensure_equals(bd.acquire(mdc[0], WritableDataset::REPLACE_HIGHER_USN), WritableDataset::ACQ_OK);

    // Acquire with a newer USN: it works
    ensure_equals(bd.acquire(mdc_upd[0], WritableDataset::REPLACE_HIGHER_USN), WritableDataset::ACQ_OK);

    // Acquire with the lower USN: it fails
    ensure_equals(bd.acquire(mdc[0], WritableDataset::REPLACE_HIGHER_USN), WritableDataset::ACQ_ERROR_DUPLICATE);

    // Acquire with the same high USN: it works, since USNs the same as the existing ones do overwrite
    ensure_equals(bd.acquire(mdc_upd[0], WritableDataset::REPLACE_HIGHER_USN), WritableDataset::ACQ_OK);

    // Try to query the element and see if it is the right one
    {
        metadata::Collection mdc_read;
        auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*cfg.section("testbufr")));
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:BUFR")), mdc_read);
        ensure_equals(mdc_read.size(), 1u);
        int usn;
        ensure_equals(scan::update_sequence_number(mdc_read[0], usn), true);
        ensure_equals(usn, 2);
    }
}

// Test a dataset with very large mock files in it
template<> template<>
void to::test<16>()
{
    // A dataset with hole files
    string conf =
        "type = ondisk2\n"
        "step = daily\n"
        "unique = reftime\n"
        "segments = holes\n"
        "name = testholes\n"
        "path = testholes\n";
    auto_ptr<dataset::WritableLocal> writer(make_dataset_writer(conf));

    // Import 24*30*10Mb=7.2Gb of data
    for (unsigned day = 1; day <= 30; ++day)
    {
        for (unsigned hour = 0; hour < 24; ++hour)
        {
            Metadata md = testdata::make_large_mock("grib", 10*1024*1024, 12, day, hour);
            WritableDataset::AcquireResult res = writer->acquire(md);
            wassert(actual(res) == WritableDataset::ACQ_OK);
        }
    }
    writer->flush();
    wassert(actual(writer.get()).check_clean());

    // Query it, without data
    std::auto_ptr<ReadonlyDataset> reader(make_dataset_reader(conf));
    metadata::Collection mdc;
    reader->queryData(dataset::DataQuery(Matcher::parse("")), mdc);
    wassert(actual(mdc.size()) == 720);

    // Query it, streaming its data to /dev/null
    ofstream out("/dev/null");
    dataset::ByteQuery bq;
    bq.setData(Matcher::parse(""));
    reader->queryBytes(bq, out);
}


}
