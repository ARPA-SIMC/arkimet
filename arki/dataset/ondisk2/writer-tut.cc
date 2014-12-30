/*
 * Copyright (C) 2007--2014  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/ondisk2/test-utils.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/source/blob.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/scan/bufr.h>
#include <arki/scan/any.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <algorithm>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace arki::utils;

namespace tut {

struct arki_dataset_ondisk2_writer_shar : public arki::tests::DatasetTest {
	arki_dataset_ondisk2_writer_shar()
	{
        system("rm -rf testdir");
		cfg.setValue("path", "testdir");
		cfg.setValue("name", "testdir");
		cfg.setValue("type", "ondisk2");
		cfg.setValue("step", "daily");
		cfg.setValue("unique", "origin, reftime");
	}

	void acquireSamples()
	{
		Metadata md;
		scan::Grib scanner;
		Writer writer(cfg);
		scanner.open("inbound/test.grib1");
		size_t count = 0;
		for ( ; scanner.next(md); ++count)
			ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		writer.flush();
	}

    Metadata& find_imported_second_in_file()
    {
        // Find the imported_result element whose offset is > 0
        UItem<types::source::Blob> second_in_segment;
        for (int i = 0; i < 3; ++i)
        {
            second_in_segment = import_results[i].source.upcast<types::source::Blob>();
            if (second_in_segment->offset > 0)
                return import_results[i];
        }
        throw wibble::exception::Consistency("second in file not found");
    }

    void import_and_make_hole(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture, std::string& holed_fname)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        wruntest(import_all, fixture);
        Metadata& md = find_imported_second_in_file();
        UItem<types::source::Blob> second_in_segment = md.source.upcast<types::source::Blob>();
        holed_fname = second_in_segment->filename.substr(8);

        {
            // Remove one element
            arki::dataset::ondisk2::Writer writer(cfg);
            writer.remove(md);
            writer.flush();
        }

        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_PACK] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }
    }

    void import_and_delete_one_file(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture, std::string& removed_fname)
    {
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        wruntest(import_all, fixture);

        // Remove all the elements in one file
        {
            arki::dataset::ondisk2::Writer writer(cfg);
            for (int i = 0; i < 3; ++i)
                if (fixture.test_data[i].destfile == fixture.test_data[0].destfile)
                    writer.remove(import_results[i]);
            writer.flush();
        }

        removed_fname = fixture.test_data[0].destfile;

        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.count_dataset_files() - 1;
            expected.by_type[COUNTED_TO_INDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }
    }

    // Test maintenance scan, on dataset with one file to pack, performing repack
    void test_hole_file_and_repack(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        string holed_fname;
        wruntest(import_and_make_hole, fixture, holed_fname);

        {
            // Test packing has something to report
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": " + holed_fname + " should be packed");
            s.require_line_contains(": 1 file should be packed");
            wassert(actual(writer.get()).repack(s, false));
        }

        // Perform packing and check that things are still ok afterwards
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": packed " + holed_fname);
            s.require_line_contains(": 1 file packed");
            wassert(actual(writer.get()).repack(s, true));

            wassert(actual(writer.get()).maintenance_clean(2));
        }

        // Ensure that we have the summary cache
        wassert(actual("testdir/.summaries/all.summary").fileexists());
        //ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
        //ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
    }

    // Test maintenance scan, on dataset with one file to delete, performing repack
    void test_delete_file_and_repack(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        string removed_fname;
        wruntest(import_and_delete_one_file, fixture, removed_fname);

        {
            // Test packing has something to report
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": " + removed_fname + " should be deleted");
            s.require_line_contains(": 1 file should be deleted");
            wassert(actual(writer.get()).repack(s, false));
        }

        // Perform packing and check that things are still ok afterwards
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": deleted " + removed_fname);
            s.require_line_contains(": 1 file deleted");
            wassert(actual(writer.get()).repack(s, true));

            wassert(actual(writer.get()).maintenance_clean(fixture.count_dataset_files() - 1));
        }

        // Ensure that we have the summary cache
        wassert(actual("testdir/.summaries/all.summary").fileexists());
        //ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
        //ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
    }

    // Test maintenance scan, on dataset with one file to pack, performing check
    void test_hole_file_and_check(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        string holed_fname;
        wruntest(import_and_make_hole, fixture, holed_fname);

        {
            // Test check has something to report
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": " + holed_fname + " should be packed");
            s.require_line_contains(": 1 file should be packed");
            wassert(actual(writer.get()).check(s, false));
        }

        // Check refuses to potentially lose data, so it does nothing in this case
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            wassert(actual(writer.get()).check_clean(true));
        }

        // In the end, we are stil left with one file to pack
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_PACK] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Ensure that we have the summary cache
        wassert(actual("testdir/.summaries/all.summary").fileexists());
        //ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
        //ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
    }

    // Test maintenance scan, on dataset with one file to delete, performing check
    void test_delete_file_and_check(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        string removed_fname;
        wruntest(import_and_delete_one_file, fixture, removed_fname);

        {
            // Test packing has something to report
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": " + removed_fname + " should be rescanned");
            s.require_line_contains(": 1 file should be rescanned");
            wassert(actual(writer.get()).check(s, false));
        }

        // Perform packing and check that things are still ok afterwards
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": rescanned " + removed_fname);
            s.require_line_contains(": 1 file rescanned");
            wassert(actual(writer.get()).check(s, true));

            wassert(actual(writer.get()).maintenance_clean(fixture.count_dataset_files()));
        }

        // Ensure that we have the summary cache
        wassert(actual("testdir/.summaries/all.summary").fileexists());
        //ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
        //ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
    }

    // Test accuracy of maintenance scan, after deleting the index
    void test_delete_index_and_check(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        wruntest(import_all_packed, fixture);

        // Query everything when the dataset is in a clean state
        metadata::Collection mdc_pre;
        {
            std::auto_ptr<ReadonlyDataset> reader(makeReader());
            reader->queryData(dataset::DataQuery(Matcher::parse(""), false), mdc_pre);
            wassert(actual(mdc_pre.size()) == 3);
        }

        sys::fs::deleteIfExists("testdir/index.sqlite");

        // All files are found as files to be indexed
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_TO_INDEX] = fixture.count_dataset_files();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // A check rebuilds the index
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            for (set<string>::const_iterator i = fixture.fnames.begin();
                    i != fixture.fnames.end(); ++i)
                s.require_line_contains(": rescanned " + *i);
            s.require_line_contains(str::fmtf(": %zd files rescanned", fixture.fnames.size()));
            wassert(actual(writer.get()).check(s, true));

            wassert(actual(writer.get()).maintenance_clean(fixture.fnames.size()));
            wassert(actual(writer.get()).repack_clean(true));
        }

        // Query everything after the rebuild and check that everything is
        // still there
        metadata::Collection mdc_post;
        {
            std::auto_ptr<ReadonlyDataset> reader(makeReader());
            reader->queryData(dataset::DataQuery(Matcher::parse(""), false), mdc_post);
            wassert(actual(mdc_post.size()) == 3);
        }

        wassert(actual(mdc_post[0]).is_similar(mdc_pre[0]));
        wassert(actual(mdc_post[1]).is_similar(mdc_pre[1]));
        wassert(actual(mdc_post[2]).is_similar(mdc_pre[2]));

        // Ensure that we have the summary cache
        wassert(actual("testdir/.summaries/all.summary").fileexists());
    }
};
TESTGRP(arki_dataset_ondisk2_writer);

template<> template<>
void to::test<1>()
{
    wruntest(test_hole_file_and_repack, testdata::GRIBData());
    wruntest(test_hole_file_and_repack, testdata::BUFRData());
    wruntest(test_hole_file_and_repack, testdata::VM2Data());
    wruntest(test_hole_file_and_repack, testdata::ODIMData());
    cfg.setValue("segments", "dir");
    wruntest(test_hole_file_and_repack, testdata::GRIBData());
    wruntest(test_hole_file_and_repack, testdata::BUFRData());
    wruntest(test_hole_file_and_repack, testdata::VM2Data());
    wruntest(test_hole_file_and_repack, testdata::ODIMData());
}

template<> template<>
void to::test<2>()
{
    wruntest(test_delete_file_and_repack, testdata::GRIBData());
    wruntest(test_delete_file_and_repack, testdata::BUFRData());
    wruntest(test_delete_file_and_repack, testdata::VM2Data());
    wruntest(test_delete_file_and_repack, testdata::ODIMData());
    cfg.setValue("segments", "dir");
    wruntest(test_delete_file_and_repack, testdata::GRIBData());
    wruntest(test_delete_file_and_repack, testdata::BUFRData());
    wruntest(test_delete_file_and_repack, testdata::VM2Data());
    wruntest(test_delete_file_and_repack, testdata::ODIMData());
}

template<> template<>
void to::test<3>()
{
    wruntest(test_hole_file_and_check, testdata::GRIBData());
    wruntest(test_hole_file_and_check, testdata::BUFRData());
    wruntest(test_hole_file_and_check, testdata::VM2Data());
    wruntest(test_hole_file_and_check, testdata::ODIMData());
    cfg.setValue("segments", "dir");
    wruntest(test_hole_file_and_check, testdata::GRIBData());
    wruntest(test_hole_file_and_check, testdata::BUFRData());
    wruntest(test_hole_file_and_check, testdata::VM2Data());
    wruntest(test_hole_file_and_check, testdata::ODIMData());
}

template<> template<>
void to::test<4>()
{
    wruntest(test_delete_file_and_check, testdata::GRIBData());
    wruntest(test_delete_file_and_check, testdata::BUFRData());
    wruntest(test_delete_file_and_check, testdata::VM2Data());
    wruntest(test_delete_file_and_check, testdata::ODIMData());
    cfg.setValue("segments", "dir");
    wruntest(test_delete_file_and_check, testdata::GRIBData());
    wruntest(test_delete_file_and_check, testdata::BUFRData());
    wruntest(test_delete_file_and_check, testdata::VM2Data());
    wruntest(test_delete_file_and_check, testdata::ODIMData());
}

template<> template<>
void to::test<5>()
{
    wruntest(test_delete_index_and_check, testdata::GRIBData());
    wruntest(test_delete_index_and_check, testdata::BUFRData());
    wruntest(test_delete_index_and_check, testdata::VM2Data());
    wruntest(test_delete_index_and_check, testdata::ODIMData());
    cfg.setValue("segments", "dir");
    wruntest(test_delete_index_and_check, testdata::GRIBData());
    wruntest(test_delete_index_and_check, testdata::BUFRData());
    wruntest(test_delete_index_and_check, testdata::VM2Data());
    wruntest(test_delete_index_and_check, testdata::ODIMData());
}

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
template<> template<>
void to::test<6>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/foo/bar/test.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

    wassert(actual(sys::fs::size("testdir/foo/bar/test.grib1")) == 44412*2);

	{
		// Test querying: reindexing should have chosen the last version of
		// duplicate items
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
		ensure_equals(mdc.size(), 1u);
        wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 51630, 34960));

        mdc.clear();
        reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
        ensure_equals(mdc.size(), 1u);
        wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 44412, 7218));
    }

    // Perform packing and check that things are still ok afterwards
    s.str(std::string());
    writer.repack(s, true);
    wassert(actual(s.str()).contains("testdir: packed foo/bar/test.grib1 (44412 saved)"));
    wassert(actual(s.str()).matches("testdir: 1 file packed, [0-9]+ total bytes freed."));
    c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

    wassert(actual(sys::fs::size("testdir/foo/bar/test.grib1")) == 44412);

	// Test querying, and see that things have moved to the beginning
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	metadata::Collection mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 0, 34960));

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 34960, 7218));

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<7>()
{
	acquireSamples();
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");
    {
        index::WContents index(cfg);
        index.open();
        index.reset("2007/07-08.grib1");
    }

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards

	// By catting test.grib1 into 07-08.grib1, we create 2 metadata that do
	// not fit in that file (1 does).
	// Because they are duplicates of metadata in other files, one cannot
	// use the order of the data in the file to determine which one is the
	// newest. The situation cannot be fixed automatically because it is
	// impossible to determine which of the two duplicates should be thrown
	// away; therefore, we can only interrupt the maintenance and raise an
	// exception calling for manual fixing.
	try {
		writer.check(s, true, true);
		ensure(false);
	} catch (wibble::exception::Consistency& ce) {
		ensure(true);
	} catch (...) {
		ensure(false);
	}
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<8>()
{
	acquireSamples();

	// Compress one data file
	{
		metadata::Collection mdc;
		Reader reader(cfg);
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		mdc.compressDataFile(1024, "metadata file testdir/2007/07-08.grib1");
		sys::fs::deleteIfExists("testdir/2007/07-08.grib1");
	}

	// The dataset should still be clean
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// The dataset should still be clean even with an accurate scan
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c, false);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Remove the index
	system("rm testdir/index.sqlite");

	// See how maintenance scan copes
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 0u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 3u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());

		stringstream s;

		// Perform full maintenance and check that things are still ok afterwards
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: rescanned 2007/07-07.grib1\n"
			"testdir: rescanned 2007/07-08.grib1\n"
			"testdir: rescanned 2007/10-09.grib1\n"
			"testdir: 3 files rescanned.\n");
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Perform packing and check that things are still ok afterwards
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Ensure that we have the summary cache
		ensure(sys::fs::exists("testdir/.summaries/all.summary"));
		ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
		ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
	}
}


// Test sanity checks on summary cache
template<> template<>
void to::test<9>()
{
	// If we are root we can always write the summary cache, so the tests
	// will fail
	if (getuid() == 0)
		return;

	acquireSamples();
	files::removeDontpackFlagfile("testdir");

	arki::dataset::ondisk2::Writer writer(cfg);

	// Dataset is ok
	{
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

    // Perform packing to regenerate the summary cache
    {
        stringstream s;
        writer.repack(s, true);
        wassert(actual(s.str()) == "");
    }

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));

	// Make one summary cache file not writable
	chmod("testdir/.summaries/all.summary", 0400);

	// Perform check and see that we detect it
	{
		stringstream s;
		writer.check(s, false, true);
		ensure_equals(s.str(), "testdir: " + sys::fs::abspath("testdir/.summaries/all.summary") + " is not writable.\n");
	}

	// Fix it
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: " + sys::fs::abspath("testdir/.summaries/all.summary") + " is not writable.\n"
			"testdir: rebuilding summary cache.\n");
	}

	// Check again and see that everything is fine
	{
		stringstream s;
		writer.check(s, false, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}
#if 0

	// Perform packing and check that things are still ok afterwards

	// Perform full maintenance and check that things are still ok afterwards
#endif

}

// Test that the summary cache is properly invalidated on import
template<> template<>
void to::test<10>()
{
	// Perform maintenance on empty dir, creating an empty summary cache
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 0u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Query the summary, there should be no data
	{
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		Summary s;
		reader.querySummary(Matcher(), s);
		ensure_equals(s.count(), 0u);
	}

	// Acquire files
	acquireSamples();

	// Query the summary again, there should be data
	{
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		Summary s;
		reader.querySummary(Matcher(), s);
		ensure_equals(s.count(), 3u);
	}
}

// Try to reproduce a bug where two conflicting BUFR files were not properly
// imported with USN handling
template<> template<>
void to::test<11>()
{
    ConfigFile cfg;
    cfg.setValue("path", "gts_temp");
    cfg.setValue("name", "gts_temp");
    cfg.setValue("type", "ondisk2");
    cfg.setValue("step", "daily");
    cfg.setValue("unique", "origin, reftime, proddef");
    cfg.setValue("filter", "product:BUFR:t=temp");
    cfg.setValue("replace", "USN");

    Metadata md;
    scan::Bufr scanner;
    Writer writer(cfg);
    scanner.open("inbound/conflicting-temp-same-usn.bufr");
    size_t count = 0;
    for ( ; scanner.next(md); ++count)
        ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
    ensure_equals(count, 2u);
    writer.flush();
}

template<> template<>
void to::test<12>()
{
    metadata::Collection mdc;
    scan::scan("inbound/test.grib1", mdc);

    {
        // Import files
        Writer writer(cfg);
        for (metadata::Collection::iterator i = mdc.begin(); i != mdc.end(); ++i)
            wassert(actual(writer.acquire(*i)) == WritableDataset::ACQ_OK);
    }

    // Append one of the GRIBs to the wrong file
    wibble::sys::Buffer buf = mdc[1].getData();
    wassert(actual(buf.size()) == 34960);
    FILE* fd = fopen("testdir/2007/10-09.grib1", "ab");
    wassert(actual(fd != NULL).istrue());
    wassert(actual(fwrite(buf.data(), buf.size(), 1, fd)) == 1);
    wassert(actual(fclose(fd)) == 0);

    // A simple rescanFile throws "manual fix is required" error
    {
        Writer writer(cfg);
        try {
            writer.rescanFile("2007/10-09.grib1");
            wrunchecked(throw std::runtime_error("rescanFile should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Delete index then run a maintenance
    wassert(actual(unlink("testdir/index.sqlite")) == 0);

    // Run maintenance check
    {
        Writer writer(cfg);
        MaintenanceCollector c;
        writer.maintenance(c);

        wassert(actual(c.fileStates.size()) == 3u);
        wassert(actual(c.count(COUNTED_OK)) == 0u);
        wassert(actual(c.count(COUNTED_TO_INDEX)) == 3u);
        wassert(actual(c.remaining()) == "");
        wassert(actual(c.isClean()).isfalse());
    }

    {
        // Perform full maintenance and check that things are still ok afterwards
        Writer writer(cfg);
        stringstream s;
        try {
            writer.check(s, true, true);
            wrunchecked(throw std::runtime_error("writer.check should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }
}

template<> template<>
void to::test<13>()
{
    metadata::Collection mdc;
    scan::scan("inbound/test.grib1", mdc);

    {
        // Import files
        Writer writer(cfg);
        for (metadata::Collection::iterator i = mdc.begin(); i != mdc.end(); ++i)
            wassert(actual(writer.acquire(*i)) == WritableDataset::ACQ_OK);
    }

    // Append one of the GRIBs to the wrong file
    wibble::sys::Buffer buf1 = mdc[1].getData();
    wibble::sys::Buffer buf2 = mdc[2].getData();
    wassert(actual(buf1.size()) == 34960);
    wassert(actual(buf2.size()) == 2234);
    FILE* fd = fopen("testdir/2007/06-06.grib1", "ab");
    wassert(actual(fd != NULL).istrue());
    wassert(actual(fwrite(buf1.data(), buf1.size(), 1, fd)) == 1);
    wassert(actual(fwrite(buf2.data(), buf2.size(), 1, fd)) == 1);
    wassert(actual(fclose(fd)) == 0);

    // A simple rescanFile throws "manual fix is required" error
    {
        Writer writer(cfg);
        try {
            writer.rescanFile("2007/06-06.grib1");
            wrunchecked(throw std::runtime_error("rescanFile should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Run maintenance check
    {
        Writer writer(cfg);
        MaintenanceCollector c;
        writer.maintenance(c);

        wassert(actual(c.fileStates.size()) == 4u);
        wassert(actual(c.count(COUNTED_OK)) == 3u);
        wassert(actual(c.count(COUNTED_TO_INDEX)) == 1u);
        wassert(actual(c.remaining()) == "");
        wassert(actual(c.isClean()).isfalse());
    }

    {
        // Perform full maintenance and check that things are still ok afterwards
        Writer writer(cfg);
        stringstream s;
        try {
            writer.check(s, true, true);
            wrunchecked(throw std::runtime_error("writer.check should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }
}

// Test packing a dataset with VM2 data
template<> template<> void to::test<14>()
{
    clean_and_import(&cfg, "inbound/test.vm2");

    // Delete something
    metadata::Collection mdc_imported;
    {
        auto_ptr<ReadonlyDataset> reader(makeReader());
        reader->queryData(DataQuery(), mdc_imported);
    }
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        for (unsigned i = 0; i < mdc_imported.size(); ++i)
        {
            if (i % 2 == 0)
                writer->remove(mdc_imported[i]);
            else
                // Load data now, since the file is going to change later
                // during packing
                mdc_imported[i].getData();
        }
    }

    // Ensure the archive has items to pack
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        arki::tests::MaintenanceResults expected(false, 2);
        expected.by_type[COUNTED_TO_PACK] = 2;
        wassert(actual(writer.get()).maintenance(expected));

        ensure(!sys::fs::exists("testdir/.archive"));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        OutputChecker s;
        writer->repack(s, true);
        s.ensure_line_contains(": packed 1987/10-31.vm2");
        s.ensure_line_contains(": packed 2011/01-01.vm2");
        s.ensure_line_contains(": 2 files packed");
        s.ensure_all_lines_seen();
    }

    // Check that the files have actually shrunk
    wassert(actual(wibble::sys::fs::stat("testdir/1987/10-31.vm2")->st_size) == 36);
    wassert(actual(wibble::sys::fs::stat("testdir/2011/01-01.vm2")->st_size) == 33);

    // Ensure the archive is now clean
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        arki::tests::MaintenanceResults expected(true, 2);
        expected.by_type[COUNTED_OK] = 2;
        wassert(actual(writer.get()).maintenance(expected));

        wassert(!actual("testdir/.archive").fileexists());
    }

    // Ensure that the data hasn't been corrupted
    metadata::Collection mdc_packed;
    {
        auto_ptr<ReadonlyDataset> reader(makeReader());
        reader->queryData(DataQuery(), mdc_packed);
    }
    wassert(actual(mdc_packed[0]).is_similar(mdc_imported[1]));
    wassert(actual(mdc_packed[1]).is_similar(mdc_imported[3]));
    wassert(actual(mdc_packed[0].getData()) == mdc_imported[1].getData());
    wassert(actual(mdc_packed[1].getData()) == mdc_imported[3].getData());
}


}

// vim:set ts=4 sw=4:
